// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.DBManager
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.CallStatement
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.schema.InvariantViolationException
import org.apache.spark.sql.lakesoul.sources.{LakeSoulSQLConf, LakeSoulSourceUtils}
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestSparkSession}
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class DDLSuite extends DDLTestBase with SharedSparkSession
  with LakeSoulSQLCommandTest {

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new LakeSoulTestSparkSession(sparkConf)
    session.conf.set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
    session.conf.set(SQLConf.DEFAULT_CATALOG.key, "lakesoul")
    session.conf.set(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, true)
    session.sparkContext.setLogLevel("ERROR")

    session
  }

  override protected def verifyDescribeTable(tblName: String): Unit = {
    val res = sql(s"DESCRIBE TABLE $tblName").collect()
    logInfo(res.map(_.toString).mkString(","))
    assert(res.takeRight(2).map(_.getString(1)) === Seq("name", "dept"))
  }

  override protected def verifyNullabilityFailure(exception: AnalysisException): Unit = {
    exception.getMessage.contains("Cannot change nullable column to non-nullable")
  }
}


abstract class DDLTestBase extends QueryTest with SQLTestUtils {

  import testImplicits._

  protected def verifyDescribeTable(tblName: String): Unit

  protected def verifyNullabilityFailure(exception: AnalysisException): Unit

  protected def getSnapshotManagement(tableLocation: String): SnapshotManagement = {
    SnapshotManagement(tableLocation)
  }

  test("create table with NOT NULL - check violation through file writing") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        sql(
          s"""
             |CREATE TABLE lakesoul_test(a LONG, b String NOT NULL)
             |USING lakesoul
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = false)
        assert(spark.table("lakesoul_test").schema === expectedSchema)

        val location = LakeSoulSourceUtils.getLakeSoulPathByTableIdentifier(
          TableIdentifier("lakesoul_test", Some("default")))
        assert(location.isDefined)
        assert(location.get == SparkUtil.makeQualifiedPath(dir.getAbsolutePath).toUri.toString)

        Seq((1L, "a")).toDF("a", "b")
          .write.format("lakesoul").mode("append").save(location.get)
        val read = spark.read.format("lakesoul").load(location.get)
        checkAnswer(read, Seq(Row(1L, "a")))

        intercept[SparkException] {
          Seq((2L, null)).toDF("a", "b")
            .write.format("lakesoul").mode("append").save(location.get)
        }
      }
      waitForTasksToFinish()
    }
  }

  test("ALTER TABLE ADD COLUMNS with NOT NULL - not supported") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        sql(
          s"""
             |CREATE TABLE lakesoul_test(a LONG)
             |USING lakesoul
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

        val expectedSchema = new StructType().add("a", LongType, nullable = true)
        assert(spark.table("lakesoul_test").schema === expectedSchema)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE lakesoul_test
               |ADD COLUMNS (b String NOT NULL, c Int)""".stripMargin)
        }
        val msg = "`NOT NULL in ALTER TABLE ADD COLUMNS` is not supported for LakeSoul tables"
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN from nullable to NOT NULL - not supported") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        sql(
          s"""
             |CREATE TABLE lakesoul_test(a LONG, b String)
             |USING lakesoul
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

        val expectedSchema = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = true)
        assert(spark.table("lakesoul_test").schema === expectedSchema)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE lakesoul_test
               |CHANGE COLUMN b b String NOT NULL""".stripMargin)
        }
        verifyNullabilityFailure(e)
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN from NOT NULL to nullable") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        sql(
          s"""
             |CREATE TABLE lakesoul_test(a LONG NOT NULL, b String)
             |USING lakesoul
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

        val expectedSchema = new StructType()
          .add("a", LongType, nullable = false)
          .add("b", StringType, nullable = true)
        assert(spark.table("lakesoul_test").schema === expectedSchema)

        sql("INSERT INTO lakesoul_test SELECT 1, 'a'")
        checkAnswer(
          sql("SELECT * FROM lakesoul_test"),
          Seq(Row(1L, "a")))

        sql(
          s"""
             |ALTER TABLE lakesoul_test
             |ALTER COLUMN a DROP NOT NULL""".stripMargin)
        val expectedSchema2 = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = true)
        assert(spark.table("lakesoul_test").schema === expectedSchema2)

        sql("INSERT INTO lakesoul_test SELECT NULL, 'b'")
        checkAnswer(
          sql("SELECT * FROM lakesoul_test"),
          Seq(Row(1L, "a"), Row(null, "b")))
      }
    }
  }

  test("create table with NOT NULL - check violation through SQL") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        sql(
          s"""
             |CREATE TABLE lakesoul_test(a LONG, b String NOT NULL)
             |USING lakesoul
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = false)
        assert(spark.table("lakesoul_test").schema === expectedSchema)

        sql("INSERT INTO lakesoul_test SELECT 1, 'a'")
        checkAnswer(
          sql("SELECT * FROM lakesoul_test"),
          Seq(Row(1L, "a")))

        val e = intercept[Exception] {
          sql("INSERT INTO lakesoul_test VALUES (2, null)")
        }
        if (!e.getMessage.contains("nullable values to non-null column")) {
          verifyInvariantViolationException(e)
        }
      }
    }
  }

  test("create table with NOT NULL in struct type - check violation") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        sql(
          s"""
             |CREATE TABLE lakesoul_test
             |(x struct<a: LONG, b: String NOT NULL>, y LONG)
             |USING lakesoul
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("x", new StructType().
            add("a", LongType, nullable = true)
            .add("b", StringType, nullable = false))
          .add("y", LongType, nullable = true)
        assert(spark.table("lakesoul_test").schema === expectedSchema)

        sql("INSERT INTO lakesoul_test SELECT (1, 'a'), 1")
        checkAnswer(
          sql("SELECT * FROM lakesoul_test"),
          Seq(Row(Row(1L, "a"), 1)))

        val location = LakeSoulSourceUtils.getLakeSoulPathByTableIdentifier(
          TableIdentifier("lakesoul_test", Some("default")))
        assert(location.isDefined)
        assert(location.get == SparkUtil.makeQualifiedPath(dir.getAbsolutePath).toUri.toString)

        val schema = new StructType()
          .add("x",
            new StructType()
              .add("a", "bigint")
              .add("b", "string"))
          .add("y", "bigint")
        val e = intercept[SparkException] {
          spark.createDataFrame(
            Seq(Row(Row(2L, null), 2L)).asJava,
            schema
          ).write.format("lakesoul").mode("append").save(location.get)
        }
        verifyInvariantViolationException(e)
      }
    }
  }

  test("ALTER TABLE ADD COLUMNS with NOT NULL in struct type - not supported") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        sql(
          s"""
             |CREATE TABLE lakesoul_test
             |(y LONG)
             |USING lakesoul
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("y", LongType, nullable = true)
        assert(spark.table("lakesoul_test").schema === expectedSchema)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE lakesoul_test
               |ADD COLUMNS (x struct<a: LONG, b: String NOT NULL>, z INT)""".stripMargin)
        }
        val msg = "Operation not allowed: " +
          "`NOT NULL in ALTER TABLE ADD COLUMNS` is not supported for LakeSoul tables"
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("ALTER TABLE ADD COLUMNS to table with existing NOT NULL fields") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        sql(
          s"""
             |CREATE TABLE lakesoul_test
             |(y LONG NOT NULL)
             |USING lakesoul
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("y", LongType, nullable = false)
        assert(spark.table("lakesoul_test").schema === expectedSchema)

        sql(
          s"""
             |ALTER TABLE lakesoul_test
             |ADD COLUMNS (x struct<a: LONG, b: String>, z INT)""".stripMargin)
        val expectedSchema2 = new StructType()
          .add("y", LongType, nullable = false)
          .add("x", new StructType()
            .add("a", LongType)
            .add("b", StringType))
          .add("z", IntegerType)
        assert(spark.table("lakesoul_test").schema === expectedSchema2)
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN with nullability change in struct type - not supported") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        sql(
          s"""
             |CREATE TABLE lakesoul_test
             |(x struct<a: LONG, b: String>, y LONG)
             |USING lakesoul
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("x", new StructType()
            .add("a", LongType)
            .add("b", StringType))
          .add("y", LongType, nullable = true)
        assert(spark.table("lakesoul_test").schema === expectedSchema)

        val e1 = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE lakesoul_test
               |CHANGE COLUMN x x struct<a: LONG, b: String NOT NULL>""".stripMargin)
        }
        assert(e1.getMessage.contains("Cannot update"))
        val e2 = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE lakesoul_test
               |CHANGE COLUMN x.b b String NOT NULL""".stripMargin) // this syntax may change
        }
        verifyNullabilityFailure(e2)
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN with nullability change in struct type - relaxed") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir { dir =>
        withTable("lakesoul_test") {
          sql(
            s"""
               |CREATE TABLE lakesoul_test
               |(x struct<a: LONG, b: String NOT NULL> NOT NULL, y LONG)
               |USING lakesoul
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
          val expectedSchema = new StructType()
            .add("x", new StructType()
              .add("a", LongType)
              .add("b", StringType, nullable = false), nullable = false)
            .add("y", LongType)
          assert(spark.table("lakesoul_test").schema === expectedSchema)
          sql("INSERT INTO lakesoul_test SELECT (1, 'a'), 1")
          checkAnswer(
            sql("SELECT * FROM lakesoul_test"),
            Seq(Row(Row(1L, "a"), 1)))

          sql(
            s"""
               |ALTER TABLE lakesoul_test
               |ALTER COLUMN x.b DROP NOT NULL""".stripMargin) // relax nullability
          sql("INSERT INTO lakesoul_test SELECT (2, null), null")
          checkAnswer(
            sql("SELECT * FROM lakesoul_test"),
            Seq(
              Row(Row(1L, "a"), 1),
              Row(Row(2L, null), null)))

          sql(
            s"""
               |ALTER TABLE lakesoul_test
               |ALTER COLUMN x DROP NOT NULL""".stripMargin)
          sql("INSERT INTO lakesoul_test SELECT null, 3")
          checkAnswer(
            sql("SELECT * FROM lakesoul_test"),
            Seq(
              Row(Row(1L, "a"), 1),
              Row(Row(2L, null), null),
              Row(null, 3)))
        }
      }
    }
  }

  private def verifyInvariantViolationException(e: Exception): Unit = {
    var violationException = e.getCause
    while (violationException != null &&
      !violationException.isInstanceOf[InvariantViolationException]) {
      violationException = violationException.getCause
    }
    if (violationException == null) {
      fail("Didn't receive a InvariantViolationException.")
    }
    assert(violationException.getMessage.contains("Invariant NOT NULL violated for column"))
  }

  test("ALTER TABLE RENAME TO - not support") {
    withTable("tbl", "newTbl") {
      sql(
        s"""
           |CREATE TABLE tbl
           |USING lakesoul
           |AS SELECT 1 as a, 'a' as b
           """.stripMargin)


      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE tbl RENAME TO newTbl")
      }
      assert(e.getMessage.contains("LakeSoul currently doesn't support rename table"))
    }
  }

  test("Call Statement") {
    withTable("lakesoul_test") {
      val call = spark.sessionState.sqlParser.parsePlan("CALL cat.system.func(c1 => 'name=name1', c2 => map('2',3), c3 => true,c4 => TIMESTAMP '2013-01-01',c5=>3L,c6=>1.0D,c7=>ARRAY(1,3))")
      val s = call.asInstanceOf[CallStatement]
      assert(s.args.length == 7)
    }
  }

  test("DESCRIBE TABLE for partitioned table") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        val path = dir.getCanonicalPath

        val df = Seq(
          (1, "IT", "Alice"),
          (2, "CS", "Bob"),
          (3, "IT", "Carol")).toDF("id", "dept", "name")
        df.write.format("lakesoul").partitionBy("name", "dept").save(path)

        sql(s"CREATE TABLE lakesoul_test USING lakesoul LOCATION '$path'")

        verifyDescribeTable("lakesoul_test")
        verifyDescribeTable(s"lakesoul.`${SparkUtil.makeQualifiedPath(path)}`")
      }
    }
  }


  test("ALTER TABLE with data CHANGE COLUMN from bigint to string") {
    withTempDir { dir =>
      withTable("lakesoul_test") {

        sql(
          s"""
             |CREATE TABLE lakesoul_test(a Long, b String)
             |USING lakesoul
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

        val expectedSchema = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = true)

        assert(spark.table("lakesoul_test").schema === expectedSchema)

        sql("INSERT INTO lakesoul_test SELECT 1, 'a'")
        assert(sql("SELECT * FROM lakesoul_test").collect()(0) == Row(1, "a"))
        LakeSoulTable.uncached(dir.getCanonicalPath)

        sql(
          s"""
             |ALTER TABLE lakesoul_test
             |CHANGE COLUMN a a String""".stripMargin)
        assert(sql("SELECT * FROM lakesoul_test").collect()(0) == Row("1", "a"))
        LakeSoulTable.uncached(dir.getCanonicalPath)


        val db = new DBManager();
        val tableInfo = db.getTableInfoByName("lakesoul_test")
        val updatedExpectedSchema = new StructType()
          .add("a", StringType, nullable = true)
          .add("b", StringType, nullable = true)
        db.updateTableSchema(tableInfo.getTableId, updatedExpectedSchema.json)
        assert(spark.table("lakesoul_test").schema === updatedExpectedSchema)
        assert(sql("SELECT * FROM lakesoul_test").collect()(0) == Row("1", "a"))
      }
    }
  }

  test("Truncate TABLE") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        val path = dir.getCanonicalPath

        val df = Seq(
          (1, "IT", "Alice"),
          (2, "CS", "Bob"),
          (3, "IT", "Carol")).toDF("id", "dept", "name")
        df.write.format("lakesoul").save(path)

        sql(s"CREATE TABLE lakesoul_test USING lakesoul LOCATION '$path'")
        checkAnswer(sql("select * from lakesoul_test"),
          Row(1, "IT", "Alice") ::
          Row(2, "CS", "Bob") ::
          Row(3, "IT", "Carol") :: Nil)

        sql("TRUNCATE TABLE lakesoul_test")
        checkAnswer(sql("select * from lakesoul_test"), Seq.empty[Row])
      }
    }
  }

  test("Truncate TABLE with partitions") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        val path = dir.getCanonicalPath

        val df = Seq(
          (1, "IT", "Alice"),
          (2, "CS", "Bob"),
          (3, "IT", "Carol")).toDF("id", "dept", "name")
        df.write.format("lakesoul").partitionBy("dept").save(path)

        sql(s"CREATE TABLE lakesoul_test USING lakesoul LOCATION '$path'")
        checkAnswer(sql("select * from lakesoul_test").select("id", "dept", "name"),
          Row(1, "IT", "Alice") ::
            Row(2, "CS", "Bob") ::
            Row(3, "IT", "Carol") :: Nil)

        sql("TRUNCATE TABLE lakesoul_test")
        checkAnswer(sql("select * from lakesoul_test"), Seq.empty[Row])
      }
    }
  }
}
