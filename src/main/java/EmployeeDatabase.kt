import model.EmployeeDatabase
import org.apache.avro.file.DataFileReader
import org.apache.avro.io.DatumWriter

import java.lang.Exception

import org.apache.avro.file.DataFileWriter
import org.apache.avro.io.DatumReader
import org.apache.avro.specific.SpecificDatumReader

import org.apache.avro.specific.SpecificDatumWriter
import java.io.*
import java.util.*


fun serialize() {
    try {
        val Emp1 = EmployeeDatabase( "ram",34.34,7, 877202)
        val userDatumWriter: DatumWriter<EmployeeDatabase> = SpecificDatumWriter(EmployeeDatabase::class.java)
        val dataFileWriter: DataFileWriter<EmployeeDatabase> = DataFileWriter(userDatumWriter)
        dataFileWriter.create(Emp1.schema, File("empOutput.txt"))
        dataFileWriter.append(Emp1)
        dataFileWriter.close()
    println("Serialization is done and data store in 'empOutput.txt' file")
    } catch (e: Exception) {
        println(e)
    }
    }
    fun deSerialize() {
        try {
            val userDatumReader: DatumReader<EmployeeDatabase> = SpecificDatumReader(EmployeeDatabase::class.java)
            val dataFileReader: DataFileReader<EmployeeDatabase> = DataFileReader(File("empOutput.txt"), userDatumReader);
            var emp: EmployeeDatabase? = null
            while (dataFileReader.hasNext()) {
                emp = dataFileReader.next(emp)
                println(emp)
            }
        } catch (e: Exception) {
            println(e)
        }
    }
        fun main() {
            val  inputKey = Scanner(System.`in`)
            print ("Enter 1 for serialize and 2 for deSerialize : ")
            when(inputKey.nextInt()){
                1 -> serialize()
                2 -> deSerialize()
            }
        }
