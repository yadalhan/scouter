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

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Hashtable
import scouter.server.Configure
import scouter.util.IClose

object PostgreSQLXLogProfileWriter {
    val table = new Hashtable[String, PostgreSQLXLogProfileWriter]()
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

    def open(date: String): PostgreSQLXLogProfileWriter = {
        table.synchronized {
            var writer = table.get(date)
            if (writer != null) {
                writer.reference += 1
            } else {
                writer = new PostgreSQLXLogProfileWriter(date)
                table.put(date, writer)
            }
            return writer
        }
    }
}

class PostgreSQLXLogProfileWriter(date: String) extends IClose {
    var reference = 0
    private val conf = Configure.getInstance()
    
    // Profile 데이터 삽입 SQL
    private val insertSQL = """
        INSERT INTO profile_data (date, time, obj_hash, tx_id, profile)
        VALUES (?, ?, ?, ?, ?::jsonb)
    """

    def write(time: Long, txid: Long, bytes: Array[Byte]): Long = {
        val conn = PostgreSQLXLogProfileWriter.getConnection()
        val stmt = conn.prepareStatement(insertSQL, java.sql.Statement.RETURN_GENERATED_KEYS)
        
        try {
            // 데이터 파싱 및 JSON 변환
            val (objHash, jsonData) = parseProfileData(bytes)
            
            stmt.setString(1, date)
            stmt.setLong(2, time)
            stmt.setInt(3, objHash)
            stmt.setLong(4, txid)
            stmt.setString(5, jsonData)
            
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
                throw new RuntimeException("Failed to write Profile data to PostgreSQL", e)
        } finally {
            if (stmt != null) stmt.close()
        }
    }

    def writeBatch(dataList: List[(Long, Long, Array[Byte])]): Unit = {
        if (dataList.isEmpty) return
        
        val conn = PostgreSQLXLogProfileWriter.getConnection()
        conn.setAutoCommit(false)
        val stmt = conn.prepareStatement(insertSQL, java.sql.Statement.RETURN_GENERATED_KEYS)
        
        try {
            dataList.foreach { case (time, txid, bytes) =>
                val (objHash, jsonData) = parseProfileData(bytes)
                
                stmt.setString(1, date)
                stmt.setLong(2, time)
                stmt.setInt(3, objHash)
                stmt.setLong(4, txid)
                stmt.setString(5, jsonData)
                
                stmt.addBatch()
            }
            
            stmt.executeBatch()
            conn.commit()
        } catch {
            case e: Exception =>
                conn.rollback()
                e.printStackTrace()
                throw new RuntimeException("Failed to write Profile batch to PostgreSQL", e)
        } finally {
            conn.setAutoCommit(true)
            if (stmt != null) stmt.close()
        }
    }

    private def parseProfileData(bytes: Array[Byte]): (Int, String) = {
        try {
            // Profile 데이터 구조 파싱
            // 현재는 기본값 반환, 실제 데이터 구조에 맞춰 수정 필요
            
            // 기본 JSON 데이터 생성
            val jsonData = buildProfileJsonData(bytes)
            
            // 임시 객체 해시 (실제 구현 필요)
            val objHash = bytes.hashCode() & 0x7FFFFFFF
            
            (objHash, jsonData)
        } catch {
            case e: Exception =>
                e.printStackTrace()
                (0, "{}")
        }
    }

    private def buildProfileJsonData(bytes: Array[Byte]): String = {
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
        PostgreSQLXLogProfileWriter.table.synchronized {
            if (this.reference == 0) {
                PostgreSQLXLogProfileWriter.table.remove(this.date)
            } else {
                this.reference -= 1
            }
        }
    }
}
