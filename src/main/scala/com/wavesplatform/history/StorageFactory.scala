package com.wavesplatform.history

import javax.sql.DataSource

import com.wavesplatform.database.SQLiteWriter
import com.wavesplatform.features.FeatureProvider
import com.wavesplatform.settings.WavesSettings
import com.wavesplatform.state2.reader.SnapshotStateReader
import com.wavesplatform.state2.{BlockchainUpdaterImpl, StateWriter}
import scorex.transaction._
import scorex.utils.Time

object StorageFactory {

  def apply(settings: WavesSettings, ds: DataSource, time: Time): (NgHistory with DebugNgHistory, StateWriter with SnapshotStateReader, BlockchainUpdater) = {
    val stateWriter = new SQLiteWriter(ds, settings.blockchainSettings.functionalitySettings)
    val bcu = new BlockchainUpdaterImpl(stateWriter, settings, time, stateWriter)
    val history: NgHistory with DebugNgHistory with FeatureProvider = bcu.historyReader
    (history, stateWriter, bcu)
  }
}
