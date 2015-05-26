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

  private var verbose: Boolean = false

  val parser = new scopt.OptionParser[Script]("Model Matrix CLI") {

    head("Model Matrix CLI")

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    // * * *  Define Model Matrix Command Line Interface                                                                 * * *
    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

    opt[Unit]('v', "verbose")
      .text("Enable verbose logging, by default all logging is turned off")
      .action { (_, s) => { verbose = true; s }}

    cmd("definition").text("manipulate model matrix definitions:").children(

      cmd("list").text("list available model matrix definitions")
        .action((_, _) => definition.List(defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: definition.List).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: definition.List).copy(dbConfig = dbConf))
        ),

      cmd("find").text("find model matrix definitions by name")
        .action((_, _) => definition.Find("", defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: definition.Find).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: definition.Find).copy(dbConfig = dbConf)),
          arg[String]("<name>").required().text("model matrix user defined name")
            .action { (n, s) => s.asInstanceOf[definition.Find].copy(n) }
        ),

      cmd("view").text("find model matrix definitions for given id")
        .action((_, _) => definition.View(-1, defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: definition.View).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: definition.View).copy(dbConfig = dbConf)),
          arg[Int]("<model-definition-id>").required().text("model matrix definition id")
            .action { (id, s) => s.asInstanceOf[definition.View].copy(id) }
        ),

      cmd("validate").text("validate model matrix configuration")
        .action((_, _) => definition.Validate(defaultMMConfig, defaultMMFeatures))
        .children(
          opt[String]('f', "features").optional().text(s"configuration path of features definitions")
            .action { (f, s) => s.asInstanceOf[definition.Validate].copy(configPath = f) },
          arg[File]("<file>").required().text("model matrix configuration file")
            .action { (f, s) => s.asInstanceOf[definition.Validate].copy(config = f.toPath) }
        ),

      cmd("add").text("add model matrix definition from configuration file")
        .action((_, _) => definition.Add(
        defaultMMConfig,
        defaultMMFeatures,
        defaultMMName,
        defaultMMComment,
        defaultDbName,
        defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: definition.Add).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: definition.Add).copy(dbConfig = dbConf)),
          opt[String]('n', "name").optional().text("model matrix definition name")
            .action { (n, s) => s.asInstanceOf[definition.Add].copy(name = Some(n)) },
          opt[String]('c', "comment").optional().text("model matrix definition comment")
            .action { (c, s) => s.asInstanceOf[definition.Add].copy(comment = Some(c)) },
          opt[String]('f', "features").optional().text(s"configuration path of features definitions")
            .action { (f, s) => s.asInstanceOf[definition.Add].copy(configPath = f) },
          arg[File]("<file>").required().text("model matrix configuration file")
            .action { (f, s) => s.asInstanceOf[definition.Add].copy(config = f.toPath) }
        )

    )

    cmd("instance").text("manipulate model matrix instances:").children(

      cmd("list").text("list model matrix instances")
        .action((_, _) => instance.List(defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: instance.List).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: instance.List).copy(dbConfig = dbConf))
        ),

      cmd("find").text("find model matrix instances by name")
        .action((_, _) => instance.Find("", defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: instance.Find).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: instance.Find).copy(dbConfig = dbConf)),
          arg[String]("<name>").required().text("model matrix user defined name")
            .action { (n, s) => s.asInstanceOf[instance.Find].copy(n) }
        ),

      cmd("viewFeatures").text("view model instance features")
        .action((_, _) => instance.ViewFeatures(-1, defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: instance.ViewFeatures).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: instance.ViewFeatures).copy(dbConfig = dbConf)),
          arg[Int]("<model-instance-id>").required().text("model matrix instance id")
            .action { (id, s) => s.asInstanceOf[instance.ViewFeatures].copy(id) }
        ),

      cmd("viewColumns").text("view model instance columns")
        .action((_, _) => instance.ViewColumns(-1, defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: instance.ViewColumns).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: instance.ViewColumns).copy(dbConfig = dbConf)),
          arg[Int]("<model-instance-id>").required().text("model matrix instance id")
            .action { (id, s) => s.asInstanceOf[instance.ViewColumns].copy(id) }
        ),

      cmd("validate").text("validate model matrix definition against input data")
        .action((_, _) => instance.Validate(0, NoSource, defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: instance.Validate).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: instance.Validate).copy(dbConfig = dbConf)),
          arg[Int]("<model-instance-id>").required().text("model matrix instance id")
            .action { (id, s) => s.asInstanceOf[instance.Validate].copy(id) },
          arg[File]("<input-source>").required().text("inout data source")
            .action { (f, s) => s.asInstanceOf[instance.Validate].copy(source = CsvSource(f.toPath)) }
        ),

      cmd("create").text("create model matrix instance based on definition and input data")
        .action((_, _) => instance.Create(
        0,
        NoSource,
        defaultMMName,
        defaultMMComment,
        10,
        defaultDbName,
        defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: instance.Create).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: instance.Create).copy(dbConfig = dbConf)),
          opt[String]('n', "name").optional().text("model matrix instance name")
            .action { (n, s) => s.asInstanceOf[instance.Create].copy(name = Some(n)) },
          opt[String]('c', "comment").optional().text("model matrix instance comment")
            .action { (c, s) => s.asInstanceOf[instance.Create].copy(comment = Some(c)) },
          opt[Int]("concurrency").optional().text("concurrency level")
            .action { (c, s) => s.asInstanceOf[instance.Create].copy(concurrencyLevel = c) },
          arg[Int]("<model-instance-id>").required().text("model matrix instance id")
            .action { (id, s) => s.asInstanceOf[instance.Create].copy(id) },
          arg[File]("<file>").required().text("model matrix configuration file")
            .action { (f, s) => s.asInstanceOf[instance.Create].copy(source = CsvSource(f.toPath)) }
        )
    )


    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    // * * *  End Of Matrix Command Line Interface                                                                       * * *
    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

    private def overrideDbName[S <: Script](createScriptWithDbName: String => (S => S)): scopt.OptionDef[String, Script] = {
      opt[String]("dbName").
        optional().
        text("Override database configuration name defined in application.conf").
        action { (dbName, script) => createScriptWithDbName(dbName)(script.asInstanceOf[S]) }
    }

    private def overrideDbConfig[S <: Script](createScriptWithDbConfig: Config => (S => S)): scopt.OptionDef[String, Script] = {
      opt[String]("dbConfig").
        optional().
        text("Override database configuration file application.conf").
        action { (dbConf, script) => createScriptWithDbConfig(ConfigFactory.load(dbConf))(script.asInstanceOf[S]) }
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
