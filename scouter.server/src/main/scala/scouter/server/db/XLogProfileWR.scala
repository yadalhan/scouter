/*
 *  Copyright 2015 the original author or authors. 
 *  @https://github.com/scouter-project/scouter
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); 
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. 
 *
 */
package scouter.server.db;
import java.util.List
import scouter.server.Logger
import scouter.server.ShutdownManager
import scouter.server.db.xlog.XLogProfileDataReader
import scouter.server.db.xlog.XLogProfileDataWriter
import scouter.server.db.xlog.XLogProfileIndex
import scouter.util.DateUtil
import scouter.util.FileUtil
import scouter.util.RequestQueue
import scouter.util.IClose
import scouter.util.IShutdown
import scouter.util.ThreadUtil
import java.io.File
import scouter.server.util.ThreadScala
import scouter.server.util.OftenAction
import scouter.server.core.ServerStat
import scouter.server.Configure

object XLogProfileWR extends IClose {
    val queue = new RequestQueue[Data](Configure.getInstance().profile_queue_size);
    
    // PostgreSQL 사용 여부 확인 (XLogWR과 동일한 방식)
    private val conf = Configure.getInstance()
    private val usePostgreSQL = conf.postgresql_enabled
    
    class ResultSet(keys: List[Long], var reader: XLogProfileDataReader) {
        var max: Int = if (keys == null) 0 else keys.size()
        var x: Int = 0;
        def hasNext() = x < max
        def readNext() = {
            if (x >= max || reader == null) null else reader.read(keys.get(x));
            x = x + 1
        }
        def close() =
            if (this.reader != null) {
                this.reader.close();
                this.reader = null
            }
    }
    val prefix = "xlog";
    class Data(_time: Long, _txid: Long, _data: Array[Byte]) {
        val time = _time
        val txid = _txid
        val data = _data
    }
    var currentDateUnit: Long = 0
    var index: XLogProfileIndex = null
    var writer: XLogProfileDataWriter = null
    
    ThreadScala.start("scouter.server.db.XLogProfileWR") {
        while (DBCtr.running) {
            val m = queue.get();
            ServerStat.put("profile.db.queue",queue.size());
            try {
                if (currentDateUnit != DateUtil.getDateUnit(m.time)) {
                    currentDateUnit = DateUtil.getDateUnit(m.time);
                    close();
                    open(DateUtil.yyyymmdd(m.time));
                }
                if (index == null) {
                    OftenAction.act("XLogWR", 10) {
                        queue.clear();
                        currentDateUnit = 0;
                    }
                    Logger.println("S141", 10, "can't open ");
                } else {
                    // PostgreSQL 활성화 시 PostgreSQL에 기록 (XLogWR 패턴)
                    if (usePostgreSQL) {
                        writePostgreSQL(m.time, m.txid, m.data)
                    } else {
                        // 파일 시스템에 기록
                        val offset = writer.write(m.data)
                        index.addByTxid(m.txid, offset);
                    }
                }
            } catch {
                case e: Throwable => e.printStackTrace()
            }
        }
        close();
    }
    
    /**
     * Profile 데이터 쓰기
     */
    def add(time: Long, txid: Long, data: Array[Byte]) {
        // 큐에 추가 (실제 쓰기는 스레드에서 처리)
        val ok = queue.put(new Data(time, txid, data));
        if (ok == false) {
            Logger.println("S142", 10, "queue exceeded!!");
        }
    }
    
    /**
     * PostgreSQL에 Profile 데이터 쓰기 (XLogWR writePostgreSQL 메서드 패턴)
     */
    private def writePostgreSQL(time: Long, txid: Long, data: Array[Byte]): Unit = {
        try {
            import scouter.server.db.xlog.PostgreSQLXLogProfileWriter
            val date = DateUtil.yyyymmdd(time)
            val pgWriter = PostgreSQLXLogProfileWriter.open(date)
            try {
                val recordId = pgWriter.write(time, txid, data)
                if (recordId <= 0) {
                    Logger.println("S147", 10, s"PostgreSQL write returned invalid record ID: $recordId")
                }
            } finally {
                pgWriter.close()
            }
        } catch {
            case e: Exception =>
                // PostgreSQL 폴백 활성화 여부 확인
                val fallbackEnabled = "true".equalsIgnoreCase(conf.getValue("postgresql_fallback_enabled", "true"))
                if (!fallbackEnabled) {
                    Logger.println("S148", 10, s"PostgreSQL write failed for txid $txid: ${e.getMessage}")
                }
        }
    }
    
    def close() {
        FileUtil.close(index);
        FileUtil.close(writer);
        writer = null;
        index = null;
    }
    
    def open(date: String) {
        try {
            val path = getDBPath(date);
            val f = new File(path);
            if (!f.exists()) {
                f.mkdirs();
            }
            val file = path + "/" + prefix;
            index = XLogProfileIndex.open(file);
            writer = XLogProfileDataWriter.open(date, file);
        } catch {
            case e: Throwable => {
                e.printStackTrace();
                throw e;
            }
        }
    }
    
    def getDBPath(date: String): String = {
        return DBCtr.getRootPath() + "/" + date + "/profile";
    }
    
    ShutdownManager.add(new IShutdown() {
        override def shutdown() {
            queue.clear();
        }
    });
}