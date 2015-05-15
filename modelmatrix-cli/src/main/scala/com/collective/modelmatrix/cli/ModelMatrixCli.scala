package com.collective.modelmatrix.cli

import java.util.concurrent.Executors

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
import scalaz.Tag

object ModelMatrixCli extends App {

  private implicit val catalogExecutionContext =
    Tag[ExecutionContext, ModelMatrixCatalog](ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(10, threadFactory("catalog-db-pool", daemon = true)))
    )

  private val defaultDbName = "modelmatrix.catalog.db"
  private val defaultDbConfig = ConfigFactory.load()

  val parser = new scopt.OptionParser[Script]("Model Matrix CLI") {

    head("Model Matrix CLI")

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    // * * *  Define Model Matrix Command Line Interface                                                                 * * *
    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

    cmd("definitions").text("manipulate model matrix definitions:").children(

      cmd("list").text("list available model matrix definitions")
        .action((_, _) => ListModelDefinitions(defaultDbName, defaultDbConfig))
        .children(
          overrideDbName(dbName => (_: ListModelDefinitions).copy(dbName = dbName)),
          overrideDbConfig(dbConf => (_: ListModelDefinitions).copy(dbConfig = dbConf))
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

  parser.parse(args, Script.noOp(parser)) match {
    case Some(server) =>
      try {
        server.run()
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
