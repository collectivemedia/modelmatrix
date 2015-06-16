package com.collective.modelmatrix.cli

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.Executors
import java.util.logging.Level

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.log4j.Logger
import org.apache.log4j.varia.NullAppender
import parquet.hadoop.ParquetOutputCommitter

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
import scalaz.Tag

object ModelMatrixCli extends App with TurnedOffParquetLogging {

  private implicit val catalogExecutionContext =
    Tag[ExecutionContext, ModelMatrixCatalog](ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(10, threadFactory("catalog-db-pool", daemon = true)))
    )

  private val defaultMMConfig = Paths.get("./model-matrix.conf")
  private val defaultMMFeatures = "features"
  private val defaultMMName: Option[String] = None
  private val defaultMMComment: Option[String] = None
  private val defaultSourceCache: Boolean = false

  private val defaultIdColumn = "id"

  private var verbose: Boolean = false

  val parser = new scopt.OptionParser[Script]("Model Matrix CLI") {

    head("Model Matrix CLI")

    opt[Unit]('v', "verbose")
      .text("Enable verbose logging, by default all logging is turned off")
      .action { (_, s) => { verbose = true; s }}

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    // * * *  Matrix Model Catalog Schema                                                                                * * *
    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

    cmd("install-schema").text("install Model Matrix catalog schema")
      .action((_, _) => db.InstallSchema())


    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    // * * *  Matrix Model Definition CLI                                                                                * * *
    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

    cmd("definition").text("manipulate model matrix definitions:").children(

      cmd("list").text("list available model matrix definitions")
        .action((_, _) => definition.ListDefinitions(defaultMMName))
        .children(
          opt[String]('n', "name").optional().text("filter by model definition name")
            .action { (n, s) => s.as[definition.ListDefinitions].copy(name = Some(n))}
        ),

      cmd("view").children(
        cmd("features").text("view model matrix definitions features")
          .action((_, _) => definition.ViewFeatures(-1))
          .children(
            opt[Int]("definition-id").required().text("model matrix definition id")
              .action { (id, s) => s.as[definition.ViewFeatures].copy(id) }
          ),

        cmd("source").text("view model matrix definitions source")
          .action((_, _) => definition.ViewSource(-1))
          .children(
            opt[Int]("definition-id").required().text("model matrix definition id")
              .action { (id, s) => s.as[definition.ViewSource].copy(id) }
          )
      ),

      cmd("validate").text("validate model matrix configuration")
        .action((_, _) => definition.ValidateConfig(defaultMMConfig, defaultMMFeatures))
        .children(
          opt[String]('f', "features").optional().text(s"configuration path of features definitions")
            .action { (f, s) => s.as[definition.ValidateConfig].copy(configPath = f) },
          opt[File]("config").required().text("model matrix configuration file")
            .action { (f, s) => s.as[definition.ValidateConfig].copy(config = f.toPath) }
        ),

      cmd("add").text("add model matrix definition from configuration file")
        .action((_, _) => definition.AddDefinition(
        defaultMMConfig,
        defaultMMFeatures,
        defaultMMName,
        defaultMMComment))
        .children(
          opt[String]('n', "name").optional().text("model matrix definition name")
            .action { (n, s) => s.as[definition.AddDefinition].copy(name = Some(n)) },
          opt[String]('c', "comment").optional().text("model matrix definition comment")
            .action { (c, s) => s.as[definition.AddDefinition].copy(comment = Some(c)) },
          opt[String]('f', "features").optional().text(s"configuration path of features definitions")
            .action { (f, s) => s.as[definition.AddDefinition].copy(configPath = f) },
          opt[File]("config").required().text("model matrix configuration file")
            .action { (f, s) => s.as[definition.AddDefinition].copy(config = f.toPath) }
        )

    )

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    // * * *  Matrix Model Instance CLI                                                                                  * * *
    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

    cmd("instance").text("manipulate model matrix instances:").children(

      cmd("list").text("list model matrix instances")
        .action((_, _) => instance.ListInstances(None, None))
        .children(
          opt[Int]('d', "definition").optional().text("filter by model definition id")
            .action { (d, s) => s.asInstanceOf[instance.ListInstances].copy(modelDefinitionId = Some(d)) },
          opt[String]('n', "name").optional().text("filter by model instance name")
            .action { (n, s) => s.asInstanceOf[instance.ListInstances].copy(name = Some(n)) }
        ),

      cmd("view").children(
        cmd("features").text("view model instance features")
          .action((_, _) => instance.ViewFeatures(-1))
          .children(
            opt[Int]("instance-id").required().text("model matrix instance id")
              .action { (id, s) => s.as[instance.ViewFeatures].copy(id) }
          ),

        cmd("columns").text("view model instance columns")
          .action((_, _) => instance.ViewColumns(-1, None, None))
          .children(
            opt[String]('f', "feature").optional().text("filter by feature name")
              .action { (f, s) => s.as[instance.ViewColumns].copy(feature = Some(f)) },
            opt[String]('g', "group").optional().text("filter by feature group")
              .action { (g, s) => s.as[instance.ViewColumns].copy(group = Some(g)) },
            opt[Int]("instance-id").required().text("model matrix instance id")
              .action { (id, s) => s.as[instance.ViewColumns].copy(id) }
          )
      ),

      cmd("validate").text("validate model matrix definition against input data")
        .action((_, _) => instance.ValidateInputData(0, NoSource, defaultSourceCache))
        .children(
          opt[Boolean]("cache").optional().text("cache source data frame")
            .action { (c, s) => s.asInstanceOf[instance.ValidateInputData].copy(cacheSource = c) },
          opt[Int]("definition-id").required().text("model matrix definition id")
            .action { (id, s) => s.as[instance.ValidateInputData].copy(id) },
          opt[String]("source").required().text("input data source")
            .validate(Source.validate)
            .action { (f, s) => s.as[instance.ValidateInputData].copy(source = Source(f)) }
        ),

      cmd("create").text("create model matrix instance based on definition and input data")
        .action((_, _) => instance.AddInstance(
        0,
        NoSource,
        defaultMMName,
        defaultMMComment,
        10,
        defaultSourceCache)
        )
        .children(
          opt[String]('n', "name").optional().text("model matrix instance name")
            .action { (n, s) => s.as[instance.AddInstance].copy(name = Some(n)) },
          opt[String]('c', "comment").optional().text("model matrix instance comment")
            .action { (c, s) => s.as[instance.AddInstance].copy(comment = Some(c)) },
          opt[Int]("concurrency").optional().text("concurrency level")
            .action { (c, s) => s.as[instance.AddInstance].copy(concurrencyLevel = c) },
          opt[Boolean]("cache").optional().text("cache source data frame")
            .action { (c, s) => s.asInstanceOf[instance.AddInstance].copy(cacheSource = c) },
          opt[Int]("definition-id").required().text("model matrix definition id")
            .action { (id, s) => s.as[instance.AddInstance].copy(id) },
          opt[String]("source").required().text("input data source")
            .validate(Source.validate)
            .action { (f, s) => s.as[instance.AddInstance].copy(source = Source(f)) }
        )
    )

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    // * * *  Matrix Model Feature Extraction CLI                                                                        * * *
    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

    cmd("featurize").text("featurize input data with model matrix instance:").children(

      cmd("validate").text("validate input data against model matrix instance")
        .action((_, _) => featurize.ValidateInputData(0, NoSource, defaultSourceCache))
        .children(
          opt[Boolean]("cache").optional().text("cache source data frame")
            .action { (c, s) => s.asInstanceOf[featurize.ValidateInputData].copy(cacheSource = c) },
          opt[Int]("instance-id").required().text("model matrix instance id")
            .action { (id, s) => s.as[featurize.ValidateInputData].copy(id) },
          opt[String]("source").required().text("input data source")
            .validate(Source.validate)
            .action { (f, s) => s.as[featurize.ValidateInputData].copy(source = Source(f)) }
        ),

      cmd("sparse").text("featurize input data to sparse feature representation")
        .action((_, _) => featurize.SparseFeaturization(
        0,
        NoSource,
        NoSink,
        defaultIdColumn,
        defaultSourceCache)
        )
        .children(
          opt[Boolean]("cache").optional().text("cache source data frame")
            .action { (c, s) => s.asInstanceOf[featurize.SparseFeaturization].copy(cacheSource = c) },
          opt[Int]("instance-id").required().text("model matrix instance id")
            .action { (id, s) => s.as[featurize.SparseFeaturization].copy(id) },
          opt[String]("source").required().text("input data source")
            .validate(Source.validate)
            .action { (f, s) => s.as[featurize.SparseFeaturization].copy(source = Source(f)) },
          opt[String]("target").required().text("output target for featurized matrix")
            .validate(Sink.validate)
            .action { (f, s) => s.as[featurize.SparseFeaturization].copy(sink = Sink(f)) },
          opt[String]("id-column").required().text("id column name")
            .action { (c, s) => s.as[featurize.SparseFeaturization].copy(idColumn = c) }
        )
    )

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    // * * *  End Of Matrix Command Line Interface                                                                       * * *
    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

    override def showUsageAsError: Unit = {
      val delimiter = Seq.fill(80)("-").mkString("")
      Console.err.println(delimiter)
      super.showUsageAsError
      Console.err.println(delimiter)
    }

  }

  // Relying on a fact that Log4j is used as Slf4j logging framework
  private def turnOffLogging(): Unit = {
    if (!verbose) {
      Logger.getRootLogger.removeAllAppenders()
      Logger.getRootLogger.addAppender(new NullAppender())
    }
  }

  parser.parse(args, Script.noOp(parser)) match {
    case Some(script) =>
      try {
        turnOffLogging()
        script.run()
      } catch {
        case NonFatal(err) =>
          Console.err.println(s"Failed to run Model Matrix tool. \nError: $err")
          err.printStackTrace(Console.err)
          System.exit(1)

        case fatal: Throwable =>
          Console.err.println(s"Fatal error: $fatal")
          fatal.printStackTrace(Console.err)
          System.exit(1)
      }

    case None => // arguments are bad, usage message will have been displayed
  }

  private def threadFactory(prefix: String, daemon: Boolean) =
    new ThreadFactoryBuilder().
      setDaemon(daemon).
      setNameFormat(s"$prefix-%d").
      build()

}

trait TurnedOffParquetLogging {

  def enableLogForwarding() {
    // Note: the parquet.Log class has a static initializer that
    // sets the java.util.logging Logger for "parquet". This
    // checks first to see if there's any handlers already set
    // and if not it creates them. If this method executes prior
    // to that class being loaded then:
    //  1) there's no handlers installed so there's none to
    // remove. But when it IS finally loaded the desired affect
    // of removing them is circumvented.
    //  2) The parquet.Log static initializer calls setUseParentHanders(false)
    // undoing the attempt to override the logging here.
    //
    // Therefore we need to force the class to be loaded.
    // This should really be resolved by Parquet.
    Class.forName(classOf[parquet.Log].getName)

    // Note: Logger.getLogger("parquet") has a default logger
    // that appends to Console which needs to be cleared.
    val parquetLogger = java.util.logging.Logger.getLogger("parquet")
    parquetLogger.getHandlers.foreach(parquetLogger.removeHandler)
    // TODO(witgo): Need to set the log level ?
    // if(parquetLogger.getLevel != null) parquetLogger.setLevel(null)
    if (!parquetLogger.getUseParentHandlers) parquetLogger.setUseParentHandlers(true)

    // Disables WARN log message in ParquetOutputCommitter.
    // See https://issues.apache.org/jira/browse/SPARK-5968 for details
    Class.forName(classOf[ParquetOutputCommitter].getName)
    java.util.logging.Logger.getLogger(classOf[ParquetOutputCommitter].getName).setLevel(Level.OFF)
  }

  enableLogForwarding()

}
