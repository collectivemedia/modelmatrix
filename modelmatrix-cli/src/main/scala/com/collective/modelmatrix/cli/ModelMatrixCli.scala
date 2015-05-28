package com.collective.modelmatrix.cli

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.Executors

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.log4j.varia.NullAppender

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
import scalaz.Tag

object ModelMatrixCli extends App {

  private implicit val catalogExecutionContext =
    Tag[ExecutionContext, ModelMatrixCatalog](ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(10, threadFactory("catalog-db-pool", daemon = true)))
    )

  private val defaultMMConfig = Paths.get("./model-matrix.conf")
  private val defaultMMFeatures = "features"
  private val defaultMMName: Option[String] = None
  private val defaultMMComment: Option[String] = None

  private val defaultDbName = "modelmatrix.catalog.db"
  private val defaultDbConfig = ConfigFactory.load()

  private val defaultIdColumn = "id"

  private var verbose: Boolean = false

  val parser = new scopt.OptionParser[Script]("Model Matrix CLI") {

    head("Model Matrix CLI")

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    // * * *  Matrix Model Definition CLI                                                                                * * *
    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

    opt[Unit]('v', "verbose")
      .text("Enable verbose logging, by default all logging is turned off")
      .action { (_, s) => { verbose = true; s }}

    cmd("definition").text("manipulate model matrix definitions:").children(

      cmd("list").text("list available model matrix definitions")
        .action((_, _) => definition.ListDefinitions(defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: definition.ListDefinitions).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: definition.ListDefinitions).copy(dbConfig = dbConf))
        ),

      cmd("find").text("find model matrix definitions by name")
        .action((_, _) => definition.FindByName("", defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: definition.FindByName).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: definition.FindByName).copy(dbConfig = dbConf)),
          arg[String]("<name>").required().text("model matrix user defined name")
            .action { (n, s) => s.as[definition.FindByName].copy(n) }
        ),

      cmd("view").text("find model matrix definitions for given id")
        .action((_, _) => definition.ViewDefinition(-1, defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: definition.ViewDefinition).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: definition.ViewDefinition).copy(dbConfig = dbConf)),
          arg[Int]("<model-definition-id>").required().text("model matrix definition id")
            .action { (id, s) => s.as[definition.ViewDefinition].copy(id) }
        ),

      cmd("validate").text("validate model matrix configuration")
        .action((_, _) => definition.ValidateConfig(defaultMMConfig, defaultMMFeatures))
        .children(
          opt[String]('f', "features").optional().text(s"configuration path of features definitions")
            .action { (f, s) => s.as[definition.ValidateConfig].copy(configPath = f) },
          arg[File]("<file>").required().text("model matrix configuration file")
            .action { (f, s) => s.as[definition.ValidateConfig].copy(config = f.toPath) }
        ),

      cmd("add").text("add model matrix definition from configuration file")
        .action((_, _) => definition.AddDefinition(
        defaultMMConfig,
        defaultMMFeatures,
        defaultMMName,
        defaultMMComment,
        defaultDbName,
        defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: definition.AddDefinition).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: definition.AddDefinition).copy(dbConfig = dbConf)),
          opt[String]('n', "name").optional().text("model matrix definition name")
            .action { (n, s) => s.as[definition.AddDefinition].copy(name = Some(n)) },
          opt[String]('c', "comment").optional().text("model matrix definition comment")
            .action { (c, s) => s.as[definition.AddDefinition].copy(comment = Some(c)) },
          opt[String]('f', "features").optional().text(s"configuration path of features definitions")
            .action { (f, s) => s.as[definition.AddDefinition].copy(configPath = f) },
          arg[File]("<file>").required().text("model matrix configuration file")
            .action { (f, s) => s.as[definition.AddDefinition].copy(config = f.toPath) }
        )

    )

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    // * * *  Matrix Model Instance CLI                                                                                  * * *
    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

    cmd("instance").text("manipulate model matrix instances:").children(

      cmd("list").text("list model matrix instances")
        .action((_, _) => instance.ListInstances(None, defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: instance.ListInstances).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: instance.ListInstances).copy(dbConfig = dbConf)),
          opt[Int]('d', "definition").optional().text("model definition id")
            .action { (d, s) => s.asInstanceOf[instance.ListInstances].copy(modelDefinitionId = Some(d)) }
        ),

      cmd("find").text("find model matrix instances by name")
        .action((_, _) => instance.FindByName("", defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: instance.FindByName).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: instance.FindByName).copy(dbConfig = dbConf)),
          arg[String]("<name>").required().text("model matrix user defined name")
            .action { (n, s) => s.as[instance.FindByName].copy(n) }
        ),

      cmd("view").children(
        cmd("features").text("view model instance features")
          .action((_, _) => instance.ViewFeatures(-1, defaultDbName, defaultDbConfig))
          .children(
            overrideDbName(dbName => (_: instance.ViewFeatures).copy(dbName = dbName)),
            overrideDbConfig(dbConf => (_: instance.ViewFeatures).copy(dbConfig = dbConf)),
            arg[Int]("<model-instance-id>").required().text("model matrix instance id")
              .action { (id, s) => s.as[instance.ViewFeatures].copy(id) }
          ),

        cmd("columns").text("view model instance columns")
          .action((_, _) => instance.ViewColumns(-1, None, None, defaultDbName, defaultDbConfig))
          .children(
            overrideDbName(dbName => (_: instance.ViewColumns).copy(dbName = dbName)),
            overrideDbConfig(dbConf => (_: instance.ViewColumns).copy(dbConfig = dbConf)),
            opt[String]('f', "feature").optional().text("filter by feature name")
              .action { (f, s) => s.as[instance.ViewColumns].copy(feature = Some(f)) },
            opt[String]('g', "group").optional().text("filter by feature group")
              .action { (g, s) => s.as[instance.ViewColumns].copy(group = Some(g)) },
            arg[Int]("<model-instance-id>").required().text("model matrix instance id")
              .action { (id, s) => s.as[instance.ViewColumns].copy(id) }
          )
      ),

      cmd("validate").text("validate model matrix definition against input data")
        .action((_, _) => instance.ValidateInputData(0, NoSource, defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: instance.ValidateInputData).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: instance.ValidateInputData).copy(dbConfig = dbConf)),
          arg[Int]("<model-definition-id>").required().text("model matrix definition id")
            .action { (id, s) => s.as[instance.ValidateInputData].copy(id) },
          arg[String]("<input-source>").required().text("input data source")
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
        defaultDbName,
        defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: instance.AddInstance).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: instance.AddInstance).copy(dbConfig = dbConf)),
          opt[String]('n', "name").optional().text("model matrix instance name")
            .action { (n, s) => s.as[instance.AddInstance].copy(name = Some(n)) },
          opt[String]('c', "comment").optional().text("model matrix instance comment")
            .action { (c, s) => s.as[instance.AddInstance].copy(comment = Some(c)) },
          opt[Int]("concurrency").optional().text("concurrency level")
            .action { (c, s) => s.as[instance.AddInstance].copy(concurrencyLevel = c) },
          arg[Int]("<model-instance-id>").required().text("model matrix instance id")
            .action { (id, s) => s.as[instance.AddInstance].copy(id) },
          arg[String]("<file>").required().text("model matrix configuration file")
            .validate(Source.validate)
            .action { (f, s) => s.as[instance.AddInstance].copy(source = Source(f)) }
        )
    )

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    // * * *  Matrix Model Feature Extraction CLI                                                                        * * *
    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

    cmd("featurize").text("featurize input data with model matrix instance:").children(

      cmd("validate").text("validate input data against model matrix instance")
        .action((_, _) => featurize.ValidateInputData(0, NoSource, defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: featurize.ValidateInputData).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: featurize.ValidateInputData).copy(dbConfig = dbConf)),
          arg[Int]("<model-instance-id>").required().text("model matrix instance id")
            .action { (id, s) => s.as[featurize.ValidateInputData].copy(id) },
          arg[String]("<input-source>").required().text("input data source")
            .validate(Source.validate)
            .action { (f, s) => s.as[featurize.ValidateInputData].copy(source = Source(f)) }
        ),

      cmd("sparse").text("featurize input data to sparse feature representation")
        .action((_, _) => featurize.SparseFeaturization(0, NoSource, NoSink, defaultIdColumn, defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: featurize.SparseFeaturization).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: featurize.SparseFeaturization).copy(dbConfig = dbConf)),
          arg[Int]("<model-instance-id>").required().text("model matrix instance id")
            .action { (id, s) => s.as[featurize.SparseFeaturization].copy(id) },
          arg[String]("<input-source>").required().text("input data source")
            .validate(Source.validate)
            .action { (f, s) => s.as[featurize.SparseFeaturization].copy(source = Source(f)) },
          arg[String]("<output-sink>").required().text("output featurized data sink")
            .validate(Sink.validate)
            .action { (f, s) => s.as[featurize.SparseFeaturization].copy(sink = Sink(f)) },
          arg[String]("<id-column>").required().text("id column name")            
            .action { (c, s) => s.as[featurize.SparseFeaturization].copy(idColumn = c) }
        )
    )

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    // * * *  End Of Matrix Command Line Interface                                                                       * * *
    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

    private def overrideDbName[S <: Script](createScriptWithDbName: String => (S => S)): scopt.OptionDef[String, Script] = {
      opt[String]("dbName").
        optional().
        text("Override database configuration name defined in application.conf").
        action { (dbName, script) => createScriptWithDbName(dbName)(script.as[S]) }
    }

    private def overrideDbConfig[S <: Script](createScriptWithDbConfig: Config => (S => S)): scopt.OptionDef[String, Script] = {
      opt[String]("dbConfig").
        optional().
        text("Override database configuration file application.conf").
        action { (dbConf, script) => createScriptWithDbConfig(ConfigFactory.load(dbConf))(script.as[S]) }
    }

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
