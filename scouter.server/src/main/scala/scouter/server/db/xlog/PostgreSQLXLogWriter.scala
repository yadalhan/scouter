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

package scouter.server.db.xlog

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Hashtable
import scouter.server.Configure
import scouter.util.IClose
import scouter.io.DataInputX

object PostgreSQLXLogWriter {
    val table = new Hashtable[String, PostgreSQLXLogWriter]()
    private var connection: Connection = _

    def getConnection(): Connection = {
        if (connection == null || connection.isClosed) {
            try {
                val conf = Configure.getInstance()
                Class.forName("org.postgresql.Driver")
                connection = DriverManager.getConnection(
                    conf.postgresql_url,
                    conf.postgresql_user,
                    conf.postgresql_password
                )
            } catch {
                case e: Exception =>
                    e.printStackTrace()
                    throw new RuntimeException("Failed to connect to PostgreSQL", e)
            }
        }
        connection
    }

    def open(date: String): PostgreSQLXLogWriter = {
        table.synchronized {
            var writer = table.get(date)
            if (writer != null) {
                writer.reference += 1
            } else {
                writer = new PostgreSQLXLogWriter(date)
                table.put(date, writer)
            }
            return writer
        }
    }
}

class PostgreSQLXLogWriter(date: String) extends IClose {
    var reference = 0
    private val conf = Configure.getInstance()
    
    // XLog 데이터 삽입 SQL
    private val insertSQL = """
        INSERT INTO xlog_data (date, time, obj_hash, tx_id, gx_id, service, elapsed, error, cpu, memory, data)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
    """

    def write(time: Long, txid: Long, gxid: Long, elapsed: Int, data: Array[Byte]): Long = {
        val conn = PostgreSQLXLogWriter.getConnection()
        val stmt = conn.prepareStatement(insertSQL, java.sql.Statement.RETURN_GENERATED_KEYS)
        
        try {
            // 데이터 파싱 및 JSON 변환
            val (objHash, service, jsonData) = parseXLogData(data)
            
            stmt.setString(1, date)
            stmt.setLong(2, time)
            stmt.setInt(3, objHash)
            stmt.setLong(4, txid)
            stmt.setLong(5, gxid)
            stmt.setLong(6, service)
            stmt.setInt(7, elapsed)
            stmt.setInt(8, 0) // error 기본값
            stmt.setInt(9, 0) // cpu 기본값
            stmt.setInt(10, 0) // memory 기본값
            stmt.setString(11, jsonData)
            
            stmt.executeUpdate()
            
            // 생성된 ID 반환
            val rs = stmt.getGeneratedKeys
            if (rs.next()) {
                return rs.getLong(1)
            }
            return -1L
        } catch {
            case e: Exception =>
                e.printStackTrace()
                throw new RuntimeException("Failed to write XLog data", e)
        } finally {
            stmt.close()
        }
    }

    def writeBatch(dataList: List[(Long, Long, Long, Int, Array[Byte])]): Unit = {
        if (dataList.isEmpty) return
        
        val conn = PostgreSQLXLogWriter.getConnection()
        conn.setAutoCommit(false)
        
        val stmt = conn.prepareStatement(insertSQL, java.sql.Statement.RETURN_GENERATED_KEYS)
        
        try {
            dataList.foreach { case (time, txid, gxid, elapsed, data) =>
                val (objHash, service, jsonData) = parseXLogData(data)
                
                stmt.setString(1, date)
                stmt.setLong(2, time)
                stmt.setInt(3, objHash)
                stmt.setLong(4, txid)
                stmt.setLong(5, gxid)
                stmt.setLong(6, service)
                stmt.setInt(7, elapsed)
                stmt.setInt(8, 0)
                stmt.setInt(9, 0)
                stmt.setInt(10, 0)
                stmt.setString(11, jsonData)
                
                stmt.addBatch()
            }
            
            stmt.executeBatch()
            conn.commit()
        } catch {
            case e: Exception =>
                conn.rollback()
                e.printStackTrace()
                throw new RuntimeException("Failed to write XLog batch", e)
        } finally {
            conn.setAutoCommit(true)
            stmt.close()
        }
    }

    private def parseXLogData(bytes: Array[Byte]): (Int, Long, String) = {
        try {
            // Scouter XLog 데이터 구조 파싱
            // 현재는 기본값 반환, 실제 데이터 구조에 맞춰 수정 필요
            val din = new DataInputX(bytes)
            
            // 객체 해시 추출 (실제 구현 필요)
            val objHash = try {
                // 데이터에서 obj_hash를 추출하는 로직
                din.readInt()
            } catch {
                case _: Exception => 0
            }
            
            // 서비스 정보 추출 (실제 구현 필요)
            val service = try {
                // 데이터에서 service를 추출하는 로직
                din.readLong()
            } catch {
                case _: Exception => 0L
            }
            
            // JSON 데이터 생성
            val jsonData = buildJsonData(bytes)
            
            (objHash, service, jsonData)
        } catch {
            case e: Exception =>
                e.printStackTrace()
                (0, 0L, "{}")
        }
    }

    private def buildJsonData(bytes: Array[Byte]): String = {
        try {
            // 바이너리 데이터를 JSON으로 변환
            val jsonObj = new org.json.simple.JSONObject()
            val map = jsonObj.asInstanceOf[java.util.Map[String, Any]]
            map.put("raw_data", java.util.Base64.getEncoder.encodeToString(bytes))
            map.put("size", java.lang.Integer.valueOf(bytes.length))
            jsonObj.toJSONString()
        } catch {
            case _: Exception => "{}"
        }
    }

    override def close() {
        PostgreSQLXLogWriter.table.synchronized {
            if (this.reference == 0) {
                PostgreSQLXLogWriter.table.remove(this.date)
            } else {
                this.reference -= 1
            }
        }
    }
}