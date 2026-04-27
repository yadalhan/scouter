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
import scala.collection.mutable.{ArrayBuffer, Map => MutableMap}
import scouter.server.Configure
import scouter.util.IClose

object PostgreSQLXLogProfileReader {
    val table = new Hashtable[String, PostgreSQLXLogProfileReader]()

    def open(date: String): PostgreSQLXLogProfileReader = {
        table.synchronized {
            var reader = table.get(date)
            if (reader != null) {
                reader.reference += 1
            } else {
                reader = new PostgreSQLXLogProfileReader(date)
                table.put(date, reader)
            }
            return reader
        }
    }
}

class PostgreSQLXLogProfileReader(date: String) extends IClose {
    var reference = 0
    private val conf = Configure.getInstance()

    def getProfile(txid: Long, max: Int): Array[Byte] = {
        val conn = PostgreSQLXLogWriter.getConnection()
        val sql = """
            SELECT profile FROM profile_data 
            WHERE date = ? AND tx_id = ? 
            ORDER BY time ASC
        """
        val stmt = conn.prepareStatement(sql)
				
		println()
		println(s"[DEBUG] PostgreSQLXLogProfileReader.getProfile: txid=${txid}, date=${date}, max=${max}")
		println()
        
        try {
            stmt.setString(1, date)
            stmt.setLong(2, txid)
            
            val rs = stmt.executeQuery()
            val out = new java.io.ByteArrayOutputStream()
            var blockCnt = 0
            
            while (rs.next()) {
                if (max > 0 && blockCnt >= max) {
                    val mStep = new scouter.lang.step.StepControl()
                    mStep.code = 0
                    mStep.message = " ** Profile Truncated ** "
                    out.write(new scouter.io.DataOutputX().writeStep(mStep).toByteArray())
                    return out.toByteArray()
                }
                
                val jsonData = rs.getString("profile")
                if (jsonData != null) {
                    val buff = convertJsonToBinary(jsonData)
                    if (buff != null && buff.length > 0) {
                        out.write(buff)
                        blockCnt += 1
                    }
                }
            }
            
            val result = out.toByteArray()
            if (result.length > 0) result else null
        } catch {
            case e: Exception =>
                e.printStackTrace()
                null
        } finally {
            if (stmt != null) stmt.close()
        }
    }

    def getMultipleProfiles(txids: List[Long]): Map[Long, Array[Byte]] = {

				println()
				println(s"[DEBUG] PostgreSQLXLogProfileReader.getMultipleProfiles: date=${date}")
				println()
		
        if (txids.isEmpty) return Map.empty

        
        val conn = PostgreSQLXLogWriter.getConnection()
        val placeholders = txids.map(_ => "?").mkString(",")
        val sql = s"""
            SELECT tx_id, profile FROM profile_data 
            WHERE date = ? AND tx_id IN ($placeholders)
            ORDER BY time DESC
        """
        val stmt = conn.prepareStatement(sql)
        val profileMap = MutableMap[Long, Array[Byte]]()
        
        try {
            stmt.setString(1, date)
            txids.zipWithIndex.foreach { case (txid, index) =>
                stmt.setLong(index + 2, txid)
            }
            
            val rs = stmt.executeQuery()
            while (rs.next()) {
                val txid = rs.getLong("tx_id")
                val jsonData = rs.getString("profile")
                if (jsonData != null) {
                    profileMap.put(txid, convertJsonToBinary(jsonData))
                }
            }
            
            profileMap.toMap
        } catch {
            case e: Exception =>
                e.printStackTrace()
                Map.empty
        } finally {
            if (stmt != null) stmt.close()
        }
    }

    def getProfilesByTimeRange(startTime: Long, endTime: Long, limit: Int = 100): List[Array[Byte]] = {
        val conn = PostgreSQLXLogWriter.getConnection()
        val sql = """
            SELECT profile FROM profile_data 
            WHERE date = ? AND time >= ? AND time <= ?
            ORDER BY time DESC
            LIMIT ?
        """
        val stmt = conn.prepareStatement(sql)
        val profiles = ArrayBuffer[Array[Byte]]()
        
        try {
            stmt.setString(1, date)
            stmt.setLong(2, startTime)
            stmt.setLong(3, endTime)
            stmt.setInt(4, limit)
            
            val rs = stmt.executeQuery()
            while (rs.next()) {
                val jsonData = rs.getString("profile")
                if (jsonData != null) {
                    profiles += convertJsonToBinary(jsonData)
                }
            }
            
            profiles.toList
        } catch {
            case e: Exception =>
                e.printStackTrace()
                List()
        } finally {
            if (stmt != null) stmt.close()
        }
    }

    def getProfilesByObject(objHash: Int, limit: Int = 50): List[(Long, Array[Byte])] = {
        val conn = PostgreSQLXLogWriter.getConnection()
        val sql = """
            SELECT tx_id, profile FROM profile_data 
            WHERE date = ? AND obj_hash = ?
            ORDER BY time DESC
            LIMIT ?
        """
        val stmt = conn.prepareStatement(sql)
        val profiles = ArrayBuffer[(Long, Array[Byte])]()
        
        try {
            stmt.setString(1, date)
            stmt.setInt(2, objHash)
            stmt.setInt(3, limit)
            
            val rs = stmt.executeQuery()
            while (rs.next()) {
                val txid = rs.getLong("tx_id")
                val jsonData = rs.getString("profile")
                if (jsonData != null) {
                    profiles += (txid -> convertJsonToBinary(jsonData))
                }
            }
            
            profiles.toList
        } catch {
            case e: Exception =>
                e.printStackTrace()
                List()
        } finally {
            if (stmt != null) stmt.close()
        }
    }

    def getProfileStats(): (Long, Long, Long) = {
        val conn = PostgreSQLXLogWriter.getConnection()
        val sql = """
            SELECT 
                COUNT(*) as total_count,
                MIN(time) as min_time,
                MAX(time) as max_time,
                AVG(LENGTH(profile::text)) as avg_size
            FROM profile_data 
            WHERE date = ?
        """
        val stmt = conn.prepareStatement(sql)
        
        try {
            stmt.setString(1, date)
            val rs = stmt.executeQuery()
            
            if (rs.next()) {
                val count = rs.getLong("total_count")
                val minTime = rs.getLong("min_time")
                val maxTime = rs.getLong("max_time")
                (count, minTime, maxTime)
            } else {
                (0, 0, 0)
            }
        } catch {
            case e: Exception =>
                e.printStackTrace()
                (0, 0, 0)
        } finally {
            if (stmt != null) stmt.close()
        }
    }

    def tableExists(): Boolean = {
        val conn = PostgreSQLXLogWriter.getConnection()
        val sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'profile_data'
            )
        """
        val stmt = conn.prepareStatement(sql)
        
        try {
            val rs = stmt.executeQuery()
            if (rs.next()) {
                rs.getBoolean(1)
            } else {
                false
            }
        } catch {
            case e: Exception =>
                e.printStackTrace()
                false
        } finally {
            if (stmt != null) stmt.close()
        }
    }

    def testConnection(): Boolean = {
        val conn = PostgreSQLXLogWriter.getConnection()
        try {
            !conn.isClosed
        } catch {
            case e: Exception =>
                e.printStackTrace()
                false
        }
    }

    private def convertJsonToBinary(jsonData: String): Array[Byte] = {
        try {
            if (jsonData == null || jsonData.isEmpty()) {
                println(s"[ERROR] convertJsonToBinary: jsonData is null or empty")
                return new Array[Byte](0)
            }
            
            // 로깅 데이터
            println(s"[DEBUG] convertJsonToBinary: Processing JSON (length=${jsonData.length}): ${jsonData}")
            
            val parser = new org.json.simple.parser.JSONParser()
            val parsedObj = parser.parse(jsonData)
            
            if (parsedObj.isInstanceOf[org.json.simple.JSONObject]) {
                val jsonObj = parsedObj.asInstanceOf[org.json.simple.JSONObject]
                if (jsonObj.containsKey("raw_data")) {
                    val base64Data = jsonObj.get("raw_data").asInstanceOf[String]
                    if (base64Data != null) {
                        println(s"[DEBUG] convertJsonToBinary: Extracted raw_data, length=${base64Data.length}")
                        println(s"[DEBUG] Base64 start: ${base64Data.take(50)}...")
                        
                        // Base64 데이터 유효성 검사
                        if (isValidBase64ForProfile(base64Data)) {
                            try {
                                val decodedBytes = java.util.Base64.getDecoder.decode(base64Data)
                                if (decodedBytes != null && decodedBytes.length > 0) {
                                    println(s"[SUCCESS] convertJsonToBinary: Successfully decoded ${decodedBytes.length} bytes")
                                    return decodedBytes
                                }
                            } catch {
                                case e: java.lang.IllegalArgumentException =>
                                    println(s"[ERROR] convertJsonToBinary: Base64 decoding failed: ${e.getMessage}")
                                case e: Exception =>
                                    println(s"[ERROR] convertJsonToBinary: Unexpected error: ${e.getMessage}")
                            }
                        } else {
                            println(s"[WARN] convertJsonToBinary: Invalid Base64 data format")
                        }
                    } else {
                        println(s"[WARN] convertJsonToBinary: raw_data value is null")
                    }
                } else {
                    println(s"[INFO] convertJsonToBinary: No raw_data field, returning JSON as bytes")
                    return jsonData.getBytes("UTF-8")
                }
            } else {
                println(s"[WARN] convertJsonToBinary: Parsed object is not a JSONObject")
            }
            
            println(s"[WARN] convertJsonToBinary: Could not extract raw_data, returning JSON as bytes")
            return jsonData.getBytes("UTF-8")
        } catch {
            case e: Exception =>
                println(s"[CRITICAL] convertJsonToBinary: Critical error: ${e.getMessage}")
                e.printStackTrace()
                new Array[Byte](0)
        }
    }
    
    private def isValidBase64ForProfile(base64Data: String): Boolean = {
        try {
            if (base64Data == null || base64Data.isEmpty()) {
                return false
            }
            
            // Base64 문자 집합 검사
            val base64Pattern = "^[A-Za-z0-9+/]*=*$"
            base64Data.matches(base64Pattern)
        } catch {
            case e: Exception =>
                false
        }
    }

    override def close() {
        PostgreSQLXLogProfileReader.table.synchronized {
            if (this.reference == 0) {
                PostgreSQLXLogProfileReader.table.remove(this.date)
            } else {
                this.reference -= 1
            }
        }
    }
}
