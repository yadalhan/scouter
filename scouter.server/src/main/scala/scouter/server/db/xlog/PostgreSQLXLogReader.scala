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
import scala.collection.mutable.ArrayBuffer
import scouter.server.Configure
import scouter.util.IClose

object PostgreSQLXLogReader {
    val table = new Hashtable[String, PostgreSQLXLogReader]()

    def open(date: String): PostgreSQLXLogReader = {
        table.synchronized {
            var reader = table.get(date)
            if (reader != null) {
                reader.reference += 1
            } else {
                reader = new PostgreSQLXLogReader(date)
                table.put(date, reader)
            }
            return reader
        }
    }
}

class PostgreSQLXLogReader(date: String) extends IClose {
    var reference = 0
    private val conf = Configure.getInstance()

    def read(id: Long): Array[Byte] = {
        val conn = PostgreSQLXLogWriter.getConnection()
        val sql = "SELECT data FROM xlog_data WHERE id = ? AND date = ?"
        
        val stmt = conn.prepareStatement(sql)
        try {
            stmt.setLong(1, id)
            stmt.setString(2, date)
            
            val rs = stmt.executeQuery()
            if (rs.next()) {
                val jsonData = rs.getString("data")
                return convertJsonToBytes(jsonData)
            }
            return null
        } catch {
            case e: Exception =>
                e.printStackTrace()
                throw new RuntimeException("Failed to read XLog data", e)
        } finally {
            stmt.close()
        }
    }

    def readByTimeRange(fromTime: Long, toTime: Long): List[(Long, Array[Byte])] = {
        val conn = PostgreSQLXLogWriter.getConnection()
        val sql = "SELECT id, time, data FROM xlog_data WHERE date = ? AND time >= ? AND time <= ? ORDER BY time"
        
        val stmt = conn.prepareStatement(sql)
        try {
            stmt.setString(1, date)
            stmt.setLong(2, fromTime)
            stmt.setLong(3, toTime)
            
            val rs = stmt.executeQuery()
            val result = ArrayBuffer[(Long, Array[Byte])]()
            
            while (rs.next()) {
                val id = rs.getLong("id")
                val time = rs.getLong("time")
                val jsonData = rs.getString("data")
                val bytes = convertJsonToBytes(jsonData)
                result += ((time, bytes))
            }
            
            result.toList
        } catch {
            case e: Exception =>
                e.printStackTrace()
                throw new RuntimeException("Failed to read XLog by time range", e)
        } finally {
            stmt.close()
        }
    }

    def readByTxid(txid: Long): List[Array[Byte]] = {
        val conn = PostgreSQLXLogWriter.getConnection()
        val sql = "SELECT data FROM xlog_data WHERE tx_id = ? AND date = ? ORDER BY time"
        
        val stmt = conn.prepareStatement(sql)
        try {
            stmt.setLong(1, txid)
            stmt.setString(2, date)
            
            val rs = stmt.executeQuery()
            val result = ArrayBuffer[Array[Byte]]()
            
            while (rs.next()) {
                val jsonData = rs.getString("data")
                result += convertJsonToBytes(jsonData)
            }
            
            result.toList
        } catch {
            case e: Exception =>
                e.printStackTrace()
                throw new RuntimeException("Failed to read XLog by txid", e)
        } finally {
            stmt.close()
        }
    }

    def readByGxid(gxid: Long): List[Array[Byte]] = {
        val conn = PostgreSQLXLogWriter.getConnection()
        val sql = "SELECT data FROM xlog_data WHERE gx_id = ? AND date = ? ORDER BY time"
        
        val stmt = conn.prepareStatement(sql)
        try {
            stmt.setLong(1, gxid)
            stmt.setString(2, date)
            
            val rs = stmt.executeQuery()
            val result = ArrayBuffer[Array[Byte]]()
            
            while (rs.next()) {
                val jsonData = rs.getString("data")
                result += convertJsonToBytes(jsonData)
            }
            
            result.toList
        } catch {
            case e: Exception =>
                e.printStackTrace()
                throw new RuntimeException("Failed to read XLog by gxid", e)
        } finally {
            stmt.close()
        }
    }

    def readByObjHash(objHash: Int, fromTime: Long, toTime: Long): List[(Long, Array[Byte])] = {
        val conn = PostgreSQLXLogWriter.getConnection()
        val sql = "SELECT time, data FROM xlog_data WHERE obj_hash = ? AND date = ? AND time >= ? AND time <= ? ORDER BY time"
        
        val stmt = conn.prepareStatement(sql)
        try {
            stmt.setInt(1, objHash)
            stmt.setString(2, date)
            stmt.setLong(3, fromTime)
            stmt.setLong(4, toTime)
            
            val rs = stmt.executeQuery()
            val result = ArrayBuffer[(Long, Array[Byte])]()
            
            while (rs.next()) {
                val time = rs.getLong("time")
                val jsonData = rs.getString("data")
                result += ((time, convertJsonToBytes(jsonData)))
            }
            
            result.toList
        } catch {
            case e: Exception =>
                e.printStackTrace()
                throw new RuntimeException("Failed to read XLog by obj_hash", e)
        } finally {
            stmt.close()
        }
    }

    private def convertJsonToBytes(jsonData: String): Array[Byte] = {
        try {
            if (jsonData == null || jsonData.isEmpty()) {
                println(s"[ERROR] convertJsonToBytes: jsonData is null or empty")
                return new Array[Byte](0)
            }
            
            // 로깅 데이터
            println()
            println(s"[DEBUG] convertJsonToBytes: Processing JSON (length=${jsonData.length}): ${jsonData}")
            println()
            
            val parser = new org.json.simple.parser.JSONParser()
            val parsedObj = parser.parse(jsonData)
            
            if (parsedObj.isInstanceOf[org.json.simple.JSONObject]) {
                val jsonObj = parsedObj.asInstanceOf[org.json.simple.JSONObject]
                if (jsonObj.containsKey("raw_data")) {
                    val base64Data = jsonObj.get("raw_data").asInstanceOf[String]
                    if (base64Data != null) {
                        println(s"[DEBUG] convertJsonToBytes: Extracted Base64 data, length=${base64Data.length}")
                        
                        // Base64 데이터 유효성 검사
                        println(s"[DEBUG] convertJsonToBytes: Validating Base64 data...")
                        val isValid = isValidBase64(base64Data)
                        println(s"[DEBUG] convertJsonToBytes: Base64 validation result: $isValid")
                        
                        if (isValid) {
                            try {
                                println(s"[DEBUG] convertJsonToBytes: Attempting to decode Base64...")
                                val decodedBytes = java.util.Base64.getDecoder.decode(base64Data)
                                if (decodedBytes != null && decodedBytes.length > 0) {
                                    println(s"[SUCCESS] convertJsonToBytes: Successfully decoded ${decodedBytes.length} bytes")
                                    return decodedBytes
                                } else {
                                    println(s"[ERROR] convertJsonToBytes: Decoded bytes are null or empty")
                                }
                            } catch {
                                case e: java.lang.IllegalArgumentException =>
                                    println(s"[ERROR] convertJsonToBytes: Base64 decoding failed with IllegalArgumentException: ${e.getMessage}")
                                case e: Exception =>
                                    println(s"[ERROR] convertJsonToBytes: Unexpected error during decoding: ${e.getMessage}")
                                    e.printStackTrace()
                            }
                        } else {
                            println(s"[ERROR] convertJsonToBytes: Base64 validation failed")
                        }
                    } else {
                        println(s"[ERROR] convertJsonToBytes: raw_data value is null")
                    }
                } else {
                    println(s"[DEBUG] convertJsonToBytes: Could not find raw_data key in JSON")
                }
            } else {
                println(s"[DEBUG] convertJsonToBytes: Parsed object is not a JSONObject")
            }
            
            println(s"[ERROR] convertJsonToBytes: Could not extract raw_data from JSON. Full data length: ${jsonData.length}")
            println()
            
            new Array[Byte](0)
        } catch {
            case e: Exception =>
                println(s"[CRITICAL] convertJsonToBytes: Critical error: ${e.getMessage}")
                e.printStackTrace()
                new Array[Byte](0)
        }
    }
    
    private def isValidBase64(base64Data: String): Boolean = {
        if (base64Data == null || base64Data.isEmpty()) {
            println(s"[DEBUG] isValidBase64: base64Data is null or empty")
            return false
        }
        
        try {
            // Base64 길이 검사 - 더 유연하게
            val length = base64Data.length
            if (length % 4 != 0) {
                println(s"[DEBUG] isValidBase64: Base64 length $length not multiple of 4. This is acceptable.")
                // 길이가 4의 배수가 아니어도 Base64 디코더가 처리할 수 있음 (패딩 문자 = 가 있을 수 있음)
            }
            
            // Base64 문자 집합 검사 - 더 유연하게
            val base64Pattern = "^[A-Za-z0-9+/=]*$"
            if (!base64Data.matches(base64Pattern)) {
                // 비 Base64 문자 찾기
                val nonBase64Chars = base64Data.filterNot(c => 
                    (c >= 'A' && c <= 'Z') ||
                    (c >= 'a' && c <= 'z') ||
                    (c >= '0' && c <= '9') ||
                    c == '+' || c == '/' || c == '='
                )
                
                if (nonBase64Chars.nonEmpty) {
                    println(s"[ERROR] isValidBase64: Contains non-Base64 characters: '${nonBase64Chars.mkString(", ")}' (hex: ${nonBase64Chars.map(_.toInt.toHexString).mkString(", ")})")
                    println(s"[DEBUG] Full Base64 data: $base64Data")
                    return false
                }
            }
            
            println(s"[DEBUG] isValidBase64: Validation passed for length $length")
            true
        } catch {
            case e: Exception =>
                println(s"[ERROR] isValidBase64: Validation error: ${e.getMessage}")
                false
        }
    }

    override def close() {
        PostgreSQLXLogReader.table.synchronized {
            if (this.reference == 0) {
                PostgreSQLXLogReader.table.remove(this.date)
            } else {
                this.reference -= 1
            }
        }
    }
}