package com.wavesplatform.database

import java.sql.Timestamp
import javax.sql.DataSource

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.wavesplatform.settings.FunctionalitySettings
import com.wavesplatform.state2.reader.{LeaseDetails, SnapshotStateReader}
import com.wavesplatform.state2.{AssetDescription, AssetInfo, BalanceSnapshot, ByteStr, Diff, LeaseInfo, OrderFillInfo, Portfolio, StateWriter}
import scalikejdbc.{DB, DBSession, using, _}
import scorex.account.{Address, AddressOrAlias, Alias, PublicKeyAccount}
import scorex.block.{Block, BlockHeader, SignerData}
import scorex.consensus.nxt.NxtLikeConsensusBlockData
import scorex.transaction.assets.IssueTransaction
import scorex.transaction.lease.LeaseTransaction
import scorex.transaction.{TransactionParser, _}

class SQLiteWriter(ds: DataSource, fs: FunctionalitySettings) extends SnapshotStateReader with StateWriter with History {

  private def readOnly[A](f: DBSession => A): A = using(DB(ds.getConnection))(_.localTx(f))

  override def scoreOf(blockId: ByteStr) = ???

  override def lastBlockHeaderAndSize = readOnly { implicit s =>
    val featureVotes: Set[Short] = Set.empty
    sql"""with block_meta as (select * from blocks order by height desc limit 1)
         |select
         |bm.block_timestamp,
         |bm.version,
         |bm.reference,
         |bm.generator_public_key, block_id,
         |bm.base_target, generation_signature,
         |count(tx.height),
         |length(bm.block_data_bytes)
         |from block_meta bm
         |left join transactions tx on tx.height = bm.height
         |group by bm.height""".stripMargin
      .map { rs =>
        (new BlockHeader(
          rs.get[Long](1),
          rs.get[Byte](2),
          rs.get[ByteStr](3),
          SignerData(PublicKeyAccount(rs.get[Array[Byte]](4)), rs.get[ByteStr](5)),
          NxtLikeConsensusBlockData(rs.get[Long](6), rs.get[ByteStr](7)),
          rs.get[Int](8),
          featureVotes
        ), rs.get[Int](9))
      }.single().apply()
  }

  override def blockHeaderAndSize(height: Int) = ???

  override def blockHeaderAndSize(blockId: ByteStr) = ???

  override def status = ???

  override def blockBytes(height: Int) = readOnly { implicit s =>
    sql"select block_data_bytes from blocks where height = ?"
      .bind(height)
      .map(_.get[Array[Byte]](1))
      .single()
      .apply()
  }

  override def blockBytes(blockId: ByteStr) = readOnly { implicit s =>
    sql"select block_data_bytes from blocks where block_id = ?"
      .bind(blockId.base58)
      .map(_.get[Array[Byte]](1))
      .single()
      .apply()
  }

  private val approvedFeaturesQuery = {
    val switchHeight = fs.doubleFeaturesPeriodsAfterHeight
    val initialInterval = fs.featureCheckBlocksPeriod
    val targetVoteCount = fs.blocksForFeatureActivation
    SQL(s"""with approval_heights as (
           |    select feature_id, count(height) vote_count,
           |    case when height > $switchHeight then ((height - 1) / ${initialInterval * 2} + 1) * ${initialInterval * 2}
           |    else ((height - 1) / $initialInterval + 1) * $initialInterval end target_height
           |    from feature_votes
           |    group by feature_id, target_height)
           |select feature_id, min(target_height) from approval_heights
           |where vote_count >= case when target_height > $switchHeight then ${targetVoteCount * 2} else $targetVoteCount end
           |group by feature_id""".stripMargin)
  }

  private def loadApprovedFeatures()(implicit s: DBSession) =
    approvedFeaturesQuery
      .map(rs => (rs.get[Short](1), rs.get[Int](2)))
      .list()
      .apply()
      .toMap

  override def approvedFeatures() = readOnly(loadApprovedFeatures()(_))
  override def activatedFeatures() = fs.preActivatedFeatures ++ (approvedFeatures() map {
    case (featureId, approvalHeight) => featureId -> (approvalHeight +
      (if (approvalHeight < fs.doubleFeaturesPeriodsAfterHeight) fs.featureCheckBlocksPeriod else 0))
  })

  override def featureVotes(height: Int) = readOnly { implicit s =>
    val windowSize = fs.featureCheckBlocksPeriod * (if (height >= fs.doubleFeaturesPeriodsAfterHeight) 2 else 1)
    val windowStart = (height - 1) / windowSize * windowSize + 1
    sql"select feature_id, count(height) from feature_votes where height between ? and ? group by feature_id"
      .bind(windowStart, windowStart + windowSize - 1)
      .map(rs => rs.get[Short](1) -> rs.get[Int](2))
      .list()
      .apply()
      .toMap
  }

  override def heightOf(blockId: ByteStr) = readOnly { implicit s =>
    sql"select height from blocks where block_id = ?"
      .bind(blockId)
      .map(_.get[Int](1))
      .single()
      .apply()
  }

  override def lastBlockIds(howMany: Int) = readOnly { implicit s =>
    sql"select block_id from blocks order by height desc limit ?"
      .bind(howMany)
      .map(_.get[ByteStr](1))
      .list()
      .apply()
  }

  private def loadScore()(implicit s: DBSession) = {
    sql"select cumulative_score from blocks order by height desc limit 1"
      .map(rs => BigInt(rs.get[String](1)))
      .single()
      .apply()
      .getOrElse(BigInt(0))
  }

  @volatile private var scoreCache = readOnly(loadScore()(_))

  override def score = scoreCache

  override def lastBlock = readOnly { implicit s =>
    sql"select block_data_bytes from blocks order by height desc limit 1"
      .map(_.get[Array[Byte]](1))
      .single()
      .apply()
      .map(bytes => Block.parseBytes(bytes).get)
  }

  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int) = ???

  override def parent(ofBlock: Block, back: Int) = ???

  override def nonZeroLeaseBalances = readOnly { implicit s =>
    sql"""with last_balance as (select address, max(height) height from lease_balances group by address)
         |select lb.* from lease_balances lb, last_balance
         |where lb.address = last_balance.address
         |and lb.height = last_balance.height
         |and (lease_in <> 0 or lease_out <> 0)""".stripMargin
      .map(rs => Address.fromString(rs.get[String](1)).right.get -> LeaseInfo(rs.get[Long](2), rs.get[Long](3)))
      .list()
      .apply()
      .toMap
  }

  override def transactionInfo(id: ByteStr) = ???

  override def containsTransaction(id: ByteStr) = readOnly { implicit s =>
    sql"select 1 from transactions where tx_id = ?"
      .bind(id)
      .map(_.get[Int](1))
      .single()
      .apply()
      .isDefined
  }

  override def addressTransactions(address: Address,
                                   types: Set[TransactionParser.TransactionType.Value],
                                   from: Int, count: Int) = readOnly { implicit s =>
    ???
  }

  private val wavesBalanceCache = CacheBuilder.newBuilder().maximumSize(100000).build[Address, java.lang.Long]()
  private def loadWavesBalance(a: Address)(implicit session: DBSession): java.lang.Long = {
    sql"""select wb.regular_balance from waves_balances wb
         |where wb.address = ?
         |order by wb.height desc
         |limit 1""".stripMargin
      .bind(a.address)
      .map(_.get[java.lang.Long](1))
      .single()
      .apply()
      .getOrElse(0L)
  }

  override def wavesBalance(a: Address) = wavesBalanceCache.get(a, () => readOnly(loadWavesBalance(a)(_)))

  override def leaseInfo(a: Address) = readOnly { implicit s =>
    sql"select lease_in, lease_out from lease_balances where address = ? order by height desc limit 1"
      .bind(a.address)
      .map(rs => LeaseInfo(rs.get[Long](1), rs.get[Long](2)))
      .single()
      .apply()
      .getOrElse(LeaseInfo(0, 0))
  }

  private val assetBalanceCache = CacheBuilder
    .newBuilder()
    .maximumSize(100000)
    .build[Address, Map[ByteStr, Long]]()

  private def loadAssetBalance(a: Address)(implicit session: DBSession) = {
    sql"""with latest_heights as (
         |  select address, asset_id, max(height) height
         |  from asset_balances
         |  where address = ? group by address, asset_id)
         |select ab.asset_id, ab.balance from asset_balances ab, latest_heights lh
         |where ab.height = lh.height
         |and ab.asset_id = lh.asset_id
         |and ab.address = lh.address""".stripMargin
      .bind(a.address)
      .map(rs => rs.get[ByteStr](1) -> rs.get[Long](2))
      .list()
      .apply()
      .toMap
  }

  private def assetBalanceWithSession(a: Address)(implicit session: DBSession) = assetBalanceCache.get(a, () => loadAssetBalance(a))

  override def assetBalance(a: Address) = assetBalanceCache.get(a, () => readOnly(loadAssetBalance(a)(_)))

  private val assetInfoCache = CacheBuilder.newBuilder()
    .recordStats()
    .maximumSize(10000)
    .build(new CacheLoader[ByteStr, Option[AssetInfo]] {
      override def load(key: ByteStr) = readOnly { implicit s =>
        sql"select reissuable, quantity from asset_quantity where asset_id = ? order by height desc limit 1"
          .bind(key)
          .map { rs => AssetInfo(rs.get[Boolean](1), rs.get[Long](2)) }
          .single()
          .apply()
      }
    })

  private val assetDescriptionCache = CacheBuilder.newBuilder()
    .recordStats()
    .maximumSize(10000)
    .build(new CacheLoader[ByteStr, Option[AssetDescription]] {
      override def load(key: ByteStr) = readOnly { implicit s =>
        sql"""select ai.issuer, ai.name, ai.decimals, min(aq.reissuable)
             |from asset_info ai, asset_quantity aq
             |where ai.asset_id = aq.asset_id
             |and ai.asset_id = ?
             |group by ai.issuer, ai.name, ai.decimals""".stripMargin
          .bind(key)
          .map { rs => AssetDescription(
            PublicKeyAccount(rs.get[Array[Byte]](1)),
            rs.get[Array[Byte]](2),
            rs.get[Int](3),
            rs.get[Boolean](4)) }
          .single()
          .apply()
      }
    })

  override def assetDescription(id: ByteStr) = assetDescriptionCache.get(id)

  @volatile private var heightCache = readOnly(loadHeight()(_))

  private def loadHeight()(implicit session: DBSession) =
    sql"select coalesce(max(height), 0) from blocks".map(_.get[Int](1)).single().apply().getOrElse(0)

  override def height = heightCache

  override def paymentTransactionIdByHash(hash: ByteStr) = readOnly { implicit s =>
    sql"select * from payment_transactions where tx_hash = ?"
      .bind(hash.arr)
      .map(rs => ByteStr(rs.get[Array[Byte]](1)))
      .single()
      .apply()
  }

  override def aliasesOfAddress(a: Address) = readOnly { implicit s =>
    sql"select alias from aliases where address = ?"
      .bind(a.address)
      .map(rs => Alias.fromString(rs.get[String](1)).right.get)
      .list()
      .apply()
  }

  override def resolveAlias(a: Alias) = readOnly { implicit s =>
    sql"select address from aliases where alias = ?"
      .bind(a.stringRepr)
      .map(rs => Address.fromString(rs.get[String](1)).right.get)
      .single()
      .apply()
  }

  override def activeLeases = readOnly { implicit s =>
    sql"""with latest_lease_status as (select lease_id, min(active) active from lease_status group by lease_id)
         |select lease_id from latest_lease_status lls where active""".stripMargin
      .map(_.get[ByteStr](1))
      .list()
      .apply()
  }

  override def leaseOverflows = readOnly { implicit s =>
    sql"""with
         |    balance_heights as (select address, max(height) height from waves_balances group by address),
         |    lease_heights as (select address, max(height) height from lease_balances group by address)
         |select lh.address, lb.lease_out from lease_heights lh
         |left join balance_heights bh using(address)
         |left join waves_balances wb on wb.address = bh.address and wb.height = bh.height
         |left join lease_balances lb on lb.address = lh.address and lb.height = lh.height
         |where ifnull(wb.regular_balance, 0) - lb.lease_out < 0""".stripMargin
      .map(rs => Address.fromString(rs.get[String](1)).right.get -> rs.get[Long](2))
      .list()
      .apply()
      .toMap
  }

  override def leasesOf(address: Address) = readOnly { implicit s =>
    sql"""with latest_lease_status as (select lease_id, min(active) active from lease_status group by lease_id)
         |select li.lease_id, li.sender_public_key, li.recipient, li.height, li.amount, lls.active
         |from lease_info li, latest_lease_status lls
         |where li.sender_address = ?
         |and lls.lease_id = li.lease_id""".stripMargin
      .bind(address.address)
      .map(rs => rs.get[ByteStr](1) -> LeaseDetails(
        PublicKeyAccount(rs.get[Array[Byte]](2)),
        AddressOrAlias.fromString(rs.get[String](3)).right.get,
        rs.get[Int](4),
        rs.get[Long](5),
        rs.get[Boolean](6)))
      .list()
      .apply()
      .toMap
  }

  override def leaseDetails(leaseId: ByteStr) = readOnly { implicit s =>
    sql"""with this_lease_status as (
         |select lease_id, min(active) active from lease_status where lease_id = ? group by lease_id)
         |select li.sender_public_key, li.recipient, li.height, li.amount, tl.active
         |from lease_info li, this_lease_status tl
         |where li.lease_id = tl.lease_id""".stripMargin
      .bind(leaseId)
      .map(rs => LeaseDetails(
        PublicKeyAccount(rs.get[Array[Byte]](1)),
        AddressOrAlias.fromString(rs.get[String](2)).right.get,
        rs.get[Int](3),
        rs.get[Long](4),
        rs.get[Boolean](5)))
      .single()
      .apply()
  }

  override def filledVolumeAndFee(orderId: ByteStr) = readOnly { implicit s =>
    sql"""with this_order as (select ? order_id)
         |select ifnull(fq.filled_quantity, 0), ifnull(fq.fee, 0) from this_order tho
         |left join filled_quantity fq on tho.order_id = fq.order_id
         |order by fq.height desc limit 1""".stripMargin
      .bind(orderId.arr)
      .map(rs => OrderFillInfo(rs.get[Long](1), rs.get[Long](2)))
      .single()
      .apply()
      .getOrElse(OrderFillInfo(0, 0))
  }

  case class B(balance: Long = 0, leaseIn: Long = 0, leaseOut: Long = 0) {
    lazy val effectiveBalance = balance + leaseIn - leaseOut
  }

  def balanceSnapshots(acc: Address, from: Int, to: Int) = {
    val bottomLimit = from
    val queryParams = Seq(bottomLimit, acc.stringRepr, acc.stringRepr, bottomLimit + 1, to)
    readOnly { implicit s =>
      sql"""with
           |earliest_balance as (
           |  select height, regular_balance from waves_balances where height <= ? and address = ? order by height desc limit 1
           |),
           |affected_balances as (
           |  select height, regular_balance from waves_balances where address = ? and height between ? and ?
           |  union all select * from  earliest_balance
           |),
           |earliest_lease as (
           |  select height, lease_in, lease_out from lease_balances where height <= ? and address = ? order by height desc limit 1
           |),
           |affected_leases as (
           |  select height, lease_in, lease_out from lease_balances where address = ? and height between ? and ?
           |  union all select * from earliest_lease
           |)
           |select coalesce(ab.height, al.height) height, ifnull(ab.regular_balance, 0), ifnull(al.lease_in, 0), ifnull(al.lease_out, 0)
           |from affected_balances ab left join affected_leases al using(height)
           |union
           |select coalesce(ab.height, al.height) height, ifnull(ab.regular_balance, 0), ifnull(al.lease_in, 0), ifnull(al.lease_out, 0)
           |from affected_leases al left join affected_balances ab using(height)
           |order by height asc""".stripMargin
        .bind(queryParams ++ queryParams: _*)
        .map(rs => BalanceSnapshot(rs.get[Int](1), rs.get[Long](2), rs.get[Long](3), rs.get[Long](4)))
        .list()
        .apply()
        .scanLeft(BalanceSnapshot(0, 0, 0, 0)) { (prevB, thisB) =>
          BalanceSnapshot(thisB.height,
            if (thisB.regularBalance == 0) prevB.regularBalance else thisB.regularBalance,
            if (thisB.leaseIn == 0) prevB.leaseIn else thisB.leaseIn,
            if (thisB.leaseOut == 0) prevB.leaseOut else thisB.leaseOut
          )
        }
    }
  }

  override def rollbackTo(targetBlockId: ByteStr) = using(DB(ds.getConnection))(_.localTx { implicit s =>
    val targetHeight = sql"select height from blocks where block_id = ?"
      .bind(targetBlockId)
      .map(_.get[Int](1))
      .single()
      .apply()

    val blocks = targetHeight.fold(Seq.empty[Block]) { h =>
      val recoveredTransactions = sql"select block_data_bytes from blocks where height > ?"
        .bind(h)
        .map(_.get[Array[Byte]](1))
        .list()
        .apply()
        .map(Block.parseBytes(_).get)

      sql"delete from blocks where height > ?"
        .bind(h)
        .update()
        .apply()

      recoveredTransactions
    }

    scoreCache = loadScore()
    heightCache = loadHeight()

    blocks
  })

  override def append(diff: Diff, block: Block): Unit = {
    using(DB(ds.getConnection)) { db =>
      db.localTx { implicit s =>
        heightCache += 1
        scoreCache += block.blockScore()

        storeBlock(block)
        storeTransactions(diff)
        storeIssuedAssets(diff)
        storeReissuedAssets(diff)
        storeFilledQuantity(diff)

        storeLeaseInfo(diff)
        sql"insert into lease_status (lease_id, active, height) values (?,?,?)"
          .batch(diff.leaseState.map { case (id, active) => Seq(id, active, height) }.toSeq: _*)
          .apply()

        storeLeaseBalances(diff)
        storeWavesBalances(diff)
        storeAssetBalances(diff)

        sql"insert into aliases (alias, address, height) values (?,?,?)"
          .batch(diff.transactions.values.collect {
            case (_, cat: CreateAliasTransaction, _) => Seq(cat.alias.stringRepr, cat.sender.toAddress.address)
          }.toSeq: _*)
          .apply()

        storeAddressTransactionIds(diff)

        if (heightCache % 2000 == 0) {
          sql"analyze".update().apply()

          val heightParams = Seq(heightCache - 4000, heightCache - 2000, heightCache - 4000)

          sql"""with last_asset_changes as (
               |    select address, asset_id from asset_balances where height between ? and ? group by address, asset_id)
               |delete from asset_balances where (address, asset_id) in last_asset_changes and height < ?""".stripMargin
            .bind(heightParams: _*)
            .update()
            .apply()

          sql"""with last_changes as (select address from waves_balances where height between ? and ? group by address)
               |delete from waves_balances where waves_balances.address in last_changes and height < ?""".stripMargin
            .bind(heightParams: _*)
            .update()
            .apply()
        }
      }
    }
  }

  private def storeWavesBalances(diff: Diff)(implicit session: DBSession): Unit = {
    val wavesBalances = diff.portfolios
      .collect { case (address, p) if p.balance != 0 =>
        address -> (p.balance + wavesBalanceCache.get(address, () => loadWavesBalance(address)(session)))
      }
      .toSeq

    sql"insert into waves_balances (address, regular_balance, height) values(?, ?, ?)"
      .batch((for {
        (address, balance) <- wavesBalances
      } yield Seq(address.stringRepr, balance, height)): _*)
      .apply()

    for ((a, b) <- wavesBalances) {
      wavesBalanceCache.put(a, b)
    }
  }

  private def storeAssetBalances(diff: Diff)(implicit session: DBSession): Unit = {
    val assetBalanceParams = for {
      (address, portfolio) <- diff.portfolios
      (assetId, balance) <- portfolio.assets
      if balance != 0
    } yield Seq(address.stringRepr, assetId, balance + assetBalanceWithSession(address).getOrElse(assetId, 0L), height): Seq[Any]

    sql"insert into asset_balances (address, asset_id, balance, height) values (?,?,?,?)"
      .batch(assetBalanceParams.toSeq: _*)
      .apply()

    diff.portfolios.foreach {
      case (a, p) =>
        val cachedPortfolio = assetBalanceWithSession(a)
        val updatedPortfolio = for {
          k <- p.assets.keySet ++ cachedPortfolio.keySet
        } yield k -> (BigInt(p.assets.getOrElse(k, 0L)) + BigInt(cachedPortfolio.getOrElse(k, 0L))).bigInteger.longValueExact()

        assetBalanceCache.put(a, updatedPortfolio.toMap)
    }
  }

  private def storeLeaseBalances(diff: Diff)(implicit session: DBSession) = {
    sql"""insert into lease_balances (address, lease_in, lease_out, height)
         |with this_lease as (select ? address)
         |select tl.address, ifnull(lb.lease_in, 0) + ?, ifnull(lb.lease_out, 0) + ?, ?
         |from this_lease tl
         |left join lease_balances lb on tl.address = lb.address
         |order by lb.height desc limit 1""".stripMargin
      .batch((for {
        (address, p) <- diff.portfolios
        if p.leaseInfo.leaseIn != 0 || p.leaseInfo.leaseOut != 0
      } yield Seq(address.address, p.leaseInfo.leaseIn, p.leaseInfo.leaseOut, height)).toSeq: _*)
      .apply()
  }

  private def storeLeaseInfo(diff: Diff)(implicit session: DBSession) = {
    sql"insert into lease_info (lease_id, sender_public_key, sender_address, recipient, amount, height) values (?,?,?,?,?,?)"
      .batch(diff.transactions.collect {
        case (_, (_, lt: LeaseTransaction, _)) =>
          Seq(lt.id(), lt.sender.publicKey, lt.sender.address, lt.recipient.stringRepr, lt.amount, height)
      }.toSeq: _*)
      .apply()
  }

  private def storeAddressTransactionIds(diff: Diff)(implicit session: DBSession) = {
    sql"insert into address_transaction_ids (address, tx_id, signature, height) values (?,?,?,?)"
      .batch((for {
        (_, (_, tx, addresses)) <- diff.transactions
        address <- addresses
      } yield tx match {
        case pt: PaymentTransaction => Seq(address.address, ByteStr(pt.hash()), pt.signature, height)
        case t: SignedTransaction => Seq(address.address, t.id(), t.signature, height)
        case gt: GenesisTransaction => Seq(address.address, gt.id(), gt.signature, height)
      }).toSeq: _*)
      .apply()
  }

  private def storeFilledQuantity(diff: Diff)(implicit session: DBSession) =
    sql"""insert into filled_quantity(order_id, filled_quantity, fee, height)
         |with this_order as (select ? order_id)
         |select tho.order_id, ifnull(fq.filled_quantity, 0) + ?, ifnull(fq.fee, 0) + ?, ? from this_order tho
         |left join filled_quantity fq on tho.order_id = fq.order_id
         |order by fq.height desc limit 1""".stripMargin
      .batch((for {
        (orderId, fillInfo) <- diff.orderFills
      } yield Seq(orderId.arr, fillInfo.volume, fillInfo.fee, height)).toSeq: _*)
      .apply()

  private def storeReissuedAssets(diff: Diff)(implicit session: DBSession): Unit = {
    sql"insert into asset_quantity (asset_id, quantity_change, reissuable, height) values (?,?,?,?)"
      .batch(diff.issuedAssets.map { case (id, ai) => Seq(id, ai.volume, ai.isReissuable, height) }.toSeq: _*)
      .apply()
  }

  private def storeIssuedAssets(diff: Diff)(implicit session: DBSession): Unit = {
    val issuedAssetParams = diff.transactions.values.collect {
      case (_, i: IssueTransaction, _) =>
        Seq(i.assetId(), i.sender.publicKey, i.decimals, i.name, i.description, height): Seq[Any]
    }.toSeq

    sql"insert into asset_info(asset_id, issuer, decimals, name, description, height) values (?,?,?,?,?,?)"
      .batch(issuedAssetParams: _*)
      .apply()

    for ((id, ai) <- diff.issuedAssets) {
      assetInfoCache.put(id, Some(ai))
      assetDescriptionCache.invalidate(id)
    }
  }

  private def storeBlock(block: Block)(implicit session: DBSession): Unit = {
    sql"""insert into blocks (
         |height, block_id, reference, version, block_timestamp, generator_address, generator_public_key,
         |base_target, generation_signature, block_data_bytes, cumulative_score)
         |values (?,?,?,?,?,?,?,?,?,?,?)""".stripMargin
      .bind(height, block.uniqueId, block.reference, block.version, new Timestamp(block.timestamp),
        block.signerData.generator.toAddress.stringRepr, block.signerData.generator.publicKey,
        block.consensusData.baseTarget, block.consensusData.generationSignature, block.bytes(), scoreCache)
      .update()
      .apply()

    sql"insert into feature_votes (height, feature_id) values (?,?)"
      .batch(block.featureVotes.map(Seq(height, _)).toSeq: _*)
      .apply()
  }

  private def storeTransactions(diff: Diff)(implicit session: DBSession) = {
    val transactionParams = Seq.newBuilder[Seq[Any]]

    diff.transactions.values.foreach {
      case (_, pt: PaymentTransaction, _) =>
        transactionParams += Seq(ByteStr(pt.hash()), pt.signature, pt.transactionType.id, height)
      case (_, t: SignedTransaction, _) =>
        transactionParams += Seq(t.id(), t.signature, t.transactionType.id, height)
      case (_, gt: GenesisTransaction, _) =>
        transactionParams += Seq(gt.id(), gt.signature, gt.transactionType.id, height)
    }

    sql"insert into transactions (tx_id, signature, tx_type, height) values (?,?,?,?)"
      .batch(transactionParams.result(): _*)
      .apply()
  }
}
