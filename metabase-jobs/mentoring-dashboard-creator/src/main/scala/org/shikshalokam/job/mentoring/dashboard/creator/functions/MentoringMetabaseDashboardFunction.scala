package org.shikshalokam.job.mentoring.dashboard.creator.functions

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.shikshalokam.job.mentoring.dashboard.creator.task.MentoringMetabaseDashboardConfig
import org.shikshalokam.job.mentoring.dashboard.creator.domain.Event
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.util.{MetabaseUtil, PostgresUtil}
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.immutable._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer



class MentoringMetabaseDashboardFunction(config: MentoringMetabaseDashboardConfig)(implicit val mapTypeInfo: TypeInformation[Event], @transient var postgresUtil: PostgresUtil = null, @transient var metabasePostgresUtil: PostgresUtil = null, @transient var metabaseUtil: MetabaseUtil = null)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[MentoringMetabaseDashboardFunction])

  override def metricsList(): List[String] = {
    List(config.metabaseDashboardCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val pgHost: String = config.pgHost
    val pgPort: String = config.pgPort
    val pgUsername: String = config.pgUsername
    val pgPassword: String = config.pgPassword
    val pgDataBase: String = config.pgDataBase
    val metabaseUrl: String = config.metabaseUrl
    val metabaseUsername: String = config.metabaseUsername
    val metabasePassword: String = config.metabasePassword
    val metabasePgDb: String = config.metabasePgDatabase
    val connectionUrl: String = s"jdbc:postgresql://$pgHost:$pgPort/$pgDataBase"
    val metabaseConnectionUrl: String = s"jdbc:postgresql://$pgHost:$pgPort/$metabasePgDb"
    postgresUtil = new PostgresUtil(connectionUrl, pgUsername, pgPassword)
    metabasePostgresUtil = new PostgresUtil(metabaseConnectionUrl, pgUsername, pgPassword)
    metabaseUtil = new MetabaseUtil(metabaseUrl, metabaseUsername, metabasePassword)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {

    println(s"***************** Start of Processing the Mentoring Metabase Dashboard Event with = ${event.tenantCode} *****************")

    val startTime = System.currentTimeMillis()
    val metaDataTable = config.dashboardMetadata
    val metabaseDatabase: String = config.metabaseDatabase
    val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
    val reportConfig: String = config.reportConfig
    val metabaseApiKey: String = config.metabaseApiKey
    val tenantCode: String = event.tenantCode
    val orgId: String = event.orgId
    val tenantUserTable = s"${tenantCode}_users"
    val tenantSessionTable: String = s"${tenantCode}_sessions"
    val tenantSessionAttendanceTable: String = s"${tenantCode}_session_attendance"
    val tenantConnectionsTable: String = s"${tenantCode}_connections"
    val tenantOrgMentorRatingTable: String = s"${tenantCode}_org_mentor_rating"
    val tenantOrgRolesTable: String = s"${tenantCode}_org_roles"
    val storedTableIds = TrieMap.empty[(Int, String), Int]
    val storedColumnIds = TrieMap.empty[(Int, String), Int]

    // Tenant Admin
    //    if (tenantCode.nonEmpty) {
    //      createTenantTabbedDashboard(tenantCode)
    //    }

    // Org Admin
    if (orgId.nonEmpty) {
      storedTableIds.clear()
      storedColumnIds.clear()
      createMentoringDashboardForOrg(orgId.toInt, tenantCode)
    }

    //    def createTenantTabbedDashboard(finalCollectionId: Int, tenantCode: String): Unit = {
    //
    //      // 1. Create dashboard
    //      val dashboardName = "Mentoring Reports"
    //      val dashboardDescription = s"Overview + Compare metrics for [$tenantCode]"
    //      val dashboardId = Utils.createDashboard(finalCollectionId, dashboardName, dashboardDescription, metabaseUtil)
    //
    //      // 2. Create tabs
    //      val tabList = List("Overview", "Compare")
    //      val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
    //
    //      // 3. Fetch filter config
    //      val filterConfigQuery = s"SELECT config FROM $reportConfig WHERE report_name='Tenant Admin' AND question_type='tenant-parameter'"
    //      val filterResults = postgresUtil.fetchData(filterConfigQuery)
    //      val slugNameToFilterMap = createFiltersFromConfig(filterResults, finalCollectionId, databaseId)
    //
    //      // 4. Process questions per tab
    //      tabList.foreach { tabName =>
    //        val reportConfigQuery = s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name='Mentoring Reports' AND report_name='Tenant Admin'"
    //
    //        val questionCardIds = ProcessTenantConstructor.ProcessAndUpdateJsonFiles(reportConfigQuery, finalCollectionId, databaseId, dashboardId, 0, 0, 0, 0, 0,
    //          tenantUserTable, tenantSessionTable, tenantSessionAttendanceTable, tenantConnectionsTable, tenantOrgMentorRatingTable, tenantOrgRolesTable,
    //          orgId.toInt, slugNameToFilterMap, metabaseUtil, postgresUtil, Some(tabIdMap(tabName)))
    //      }
    //
    //      // 5. Map filters to dashboard
    //      UpdateParameters.updateDashboardParameters(metabaseUtil, postgresUtil, filterConfigQuery, dashboardId, slugNameToFilterMap)
    //
    //      println(s"=====> Tenant Tabbed Dashboard created successfully for [$tenantCode]")
    //    }

    //    def createTenantTabbedDashboard(tenantCode: String): Unit = {
    //      val (collectionName, collectionDescription) =
    //        (s"Mentoring Activity $tenantCode", "Dashboards for Tenant Admin with Overview and Compare tabs.\n\nCollection For: Admin")
    //      val collectionId = Utils.checkAndCreateCollection(collectionName, collectionDescription, metabaseUtil)
    //      if (collectionId != -1) {
    //        Utils.createGroupForCollection(metabaseUtil, "Tenant_Admin_Mentoring_Activity", collectionId)
    //        val (dashboardName, dashboardDescription) =
    //          ("Mentoring Reports", s"Overview + Compare metrics for [$tenantCode]")
    //        val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, dashboardDescription, metabaseUtil)
    //        val tabList = List("Overview", "Compare Organizations")
    //        val tabIdMap = Utils.createTabs(dashboardId, tabList, metabaseUtil)
    //        val databaseId: Int = Utils.getDatabaseId(metabaseDatabase, metabaseUtil)
    //        val createDashboardQuery =
    //          s"UPDATE $metaDataTable SET status = 'Failed' WHERE entity_id = '${tenantCode}_tenant_admin';"
    //        val orgNameId: Int = getTheColumnId(databaseId, tenantOrgRolesTable, "org_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
    //        val filterConfigQuery =
    //          s"SELECT config FROM $reportConfig WHERE report_name='Tenant Admin' AND question_type='tenant-parameter'"
    //        val filterResults = postgresUtil.fetchData(filterConfigQuery)
    //        val slugNameToFilterMap = createFiltersFromConfig(filterResults, collectionId, databaseId)
    //
    //        tabList.foreach { tabName =>
    //          val reportConfigQuery =
    //            s"""
    //               |SELECT question_type, config
    //               |FROM $reportConfig
    //               |WHERE dashboard_name = 'Mentoring Reports'
    //               |AND report_name = 'Tenant Admin'
    //               |AND question_type IN ('big-number','graph')
    //         """.stripMargin
    //
    //          val questionCardIdList = ProcessTenantConstructor.ProcessAndUpdateJsonFiles(reportConfigQuery, collectionId, databaseId, dashboardId, 0, 0, 0, 0, 0, tenantUserTable,
    //            tenantSessionTable, tenantSessionAttendanceTable, tenantConnectionsTable, tenantOrgMentorRatingTable,
    //            tenantOrgRolesTable, orgId.toInt, slugNameToFilterMap, metabaseUtil, postgresUtil, Some(tabIdMap(tabName)))
    //          val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"
    //          val parametersQuery =
    //            s"SELECT config FROM $reportConfig WHERE report_name='Tenant Admin' AND question_type='tenant-parameter'"
    //          UpdateParameters.updateDashboardParameters(metabaseUtil, postgresUtil, parametersQuery, dashboardId, slugNameToFilterMap)
    //          val objectMapper = new ObjectMapper()
    //          val userMetadataJson = objectMapper.createArrayNode()
    //            .add(objectMapper.createObjectNode()
    //              .put("collectionId", collectionId)
    //              .put("collectionName", collectionName)
    //              .put("dashboardId", dashboardId)
    //              .put("dashboardName", dashboardName)
    //              .put("collectionFor", "Tenant Admin")
    //              .put("questionIds", questionIdsString)
    //            )
    //          val updateMetadataQuery =
    //            s"""
    //               |UPDATE $metaDataTable
    //               |SET main_metadata = COALESCE(main_metadata::jsonb, '[]'::jsonb) || '$userMetadataJson'::jsonb,
    //               |    status = 'Success'
    //               |WHERE entity_id = '${tenantCode}_tenant_admin';
    //         """.stripMargin
    //
    //          postgresUtil.insertData(updateMetadataQuery)
    //        }
    //
    //        println(s"=====> Dashboard '$dashboardName' created and metadata updated successfully.")
    //      }
    //    }


    def createMentoringDashboardForOrg(orgId: Int, tenantCode: String): Unit = {
      val collectionName = s"Mentoring Activity $orgId"
      val collectionDescription = s"This report has access to a dedicated dashboard offering insights and metrics specific to their own org. \n\nCollection For: Org Admin $orgId"
      val collectionId = Utils.checkAndCreateCollection(collectionName, collectionDescription, metabaseUtil)

      if (collectionId != -1) {
        Utils.createGroupForCollection(metabaseUtil, s"Org_Admin_$orgId", collectionId)

        val dashboardName = "Org Mentoring Reports"
        val dashboardDescription = s"Overview of Mentoring Across Org_$orgId"
        val dashboardId: Int = Utils.createDashboard(collectionId, dashboardName, dashboardDescription, metabaseUtil)

        val createDashboardQuery = s"UPDATE $metaDataTable SET status = 'Failed' WHERE entity_id = 'org_admin_$orgId';"
        val stateNameId = getTheColumnId(databaseId, tenantUserTable, "user_profile_one_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val districtNameId = getTheColumnId(databaseId, tenantUserTable, "user_profile_two_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val blockNameId = getTheColumnId(databaseId, tenantUserTable, "user_profile_three_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val clusterNameId = getTheColumnId(databaseId, tenantUserTable, "user_profile_four_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)
        val schoolNameId = getTheColumnId(databaseId, tenantUserTable, "user_profile_five_name", metabaseUtil, metabasePostgresUtil, metabaseApiKey, createDashboardQuery)

        metabaseUtil.updateColumnCategory(stateNameId, "State")
        metabaseUtil.updateColumnCategory(districtNameId, "City")

        // Step 1: create question cards
        val reportConfigQuery =
          s"SELECT question_type, config FROM $reportConfig WHERE dashboard_name = 'Mentoring-Reports' AND report_name = 'Org Admin' AND question_type IN ('big-number', 'graph');"

        val questionCardIdList =
          ProcessOrgConstructor.ProcessAndUpdateJsonFiles(
            reportConfigQuery, collectionId, databaseId, dashboardId,
            stateNameId, districtNameId, blockNameId, clusterNameId, schoolNameId,
            tenantUserTable, tenantSessionTable, tenantSessionAttendanceTable,
            tenantConnectionsTable, tenantOrgMentorRatingTable, tenantOrgRolesTable,
            orgId, Map.empty, metabaseUtil, postgresUtil
          ).toList

        // Step 2: fetch filter mappings (slug -> existing cardId)
        val parametersQuery =
          s"SELECT config FROM $reportConfig WHERE report_name = 'Org Admin' AND question_type = 'org-parameters'"

        // Step 3: map filters to question cards
        UpdateParameters.UpdateAdminParameterFunction(metabaseUtil, parametersQuery, dashboardId, postgresUtil)
        val questionIdsString = "[" + questionCardIdList.mkString(",") + "]"

//        UpdateParameters.pushQuestionCardsToDashboard(metabaseUtil, dashboardId, questionCards)

        val mainMetadataJson = new ObjectMapper().createObjectNode()
          .put("collectionId", collectionId)
          .put("collectionName", collectionName)
          .put("dashboardId", dashboardId)
          .put("dashboardName", dashboardName)
          .put("collectionFor", "Org Admin")
          .put("questionIds", questionIdsString)

        postgresUtil.insertData(s"UPDATE $metaDataTable SET main_metadata = '$mainMetadataJson' WHERE entity_id = 'org_admin_$orgId';")

        println(s"=====> Dashboard '$dashboardName' created and metadata updated for tenant [$tenantCode].")
      }
    }


    def createFiltersFromConfig(filterResults: List[Map[String, Any]], collectionId: Int, databaseId: Int): Map[String, Int] = {
      val objectMapper = new ObjectMapper()
      val slugNameToFilterMap = mutable.Map[String, Int]()
      for (result <- filterResults) {
        val configString = result.get("config").map(_.toString).getOrElse("")
        val configJson = objectMapper.readTree(configString)
        val slugName = configJson.findValue("name").asText()
        val filterId = AddMetadataFilter.updateAndAddFilter(metabaseUtil, configJson, collectionId, databaseId, tenantUserTable, tenantSessionTable, tenantSessionAttendanceTable, tenantConnectionsTable, tenantOrgMentorRatingTable, tenantOrgRolesTable, orgId.toInt)
        slugNameToFilterMap(slugName) = filterId
      }
      slugNameToFilterMap.toMap
    }


    def getTheTableId(databaseId: Int, tableName: String, metabaseUtil: MetabaseUtil, metabasePostgresUtil: PostgresUtil, metabaseApiKey: String): Int = {
      storedTableIds.get((databaseId, tableName)) match {
        case Some(tableId) =>
          println(s"[CACHE-HIT] Table ID for '$tableName' in DB $databaseId = $tableId")
          tableId

        case None =>
          // ✅ Important fix: restrict to correct database
          val tableQuery =
            s"SELECT id FROM metabase_table WHERE name = '$tableName' AND db_id = $databaseId;"

          val tableIdOpt = metabasePostgresUtil.fetchData(tableQuery) match {
            case List(map: Map[_, _]) =>
              map.get("id").flatMap(id => scala.util.Try(id.toString.toInt).toOption)
            case _ => None
          }

          val tableId = tableIdOpt.getOrElse {
            println(s"[WARN] Table '$tableName' not found in DB $databaseId. Trying to sync with Metabase API.")
            val tableJson = metabaseUtil.syncNewTable(databaseId, tableName, metabaseApiKey)
            val newTableId = tableJson("id").num.toInt
            println(s"[SYNCED] Table '$tableName' synced with Metabase. New table_id = $newTableId")
            newTableId
          }

          storedTableIds.put((databaseId, tableName), tableId)
          println(s"[INFO] Using table_id = $tableId for table '$tableName' in DB $databaseId")
          tableId
      }
    }

    def getTheColumnId(databaseId: Int, tableName: String, columnName: String, metabaseUtil: MetabaseUtil, metabasePostgresUtil: PostgresUtil, metabaseApiKey: String, metaTableQuery: String): Int = {
      try {
        val tableId = getTheTableId(databaseId, tableName, metabaseUtil, metabasePostgresUtil, metabaseApiKey)

        storedColumnIds.get((tableId, columnName)) match {
          case Some(columnId) =>
            println(s"[CACHE-HIT] Column '$columnName' found in table '$tableName' (tableId: $tableId) = $columnId")
            columnId

          case None =>
            val columnQuery = s"SELECT id FROM metabase_field WHERE table_id = $tableId AND name = '$columnName';"

            val columnIdOpt = metabasePostgresUtil.fetchData(columnQuery) match {
              case List(map: Map[_, _]) =>
                map.get("id").flatMap(id => scala.util.Try(id.toString.toInt).toOption)
              case _ => None
            }

            val columnId = columnIdOpt.getOrElse(-1)

            if (columnId != -1) {
              storedColumnIds.put((tableId, columnName), columnId)
              println(s"[INFO] Using column_id = $columnId for column '$columnName' in table '$tableName' (tableId: $tableId)")
              columnId
            } else {
              val errorMessage = s"Column '$columnName' not found in table '$tableName' (tableId: $tableId)"
              val escapedError = errorMessage.replace("'", "''")
              val updateTableQuery = metaTableQuery.replace("'errorMessage'", s"'$escapedError'")
              postgresUtil.insertData(updateTableQuery)
              println(s"[WARN] $errorMessage")
              -1
            }
        }
      } catch {
        case e: Exception =>
          val escapedError = e.getMessage.replace("'", "''")
          val updateTableQuery = metaTableQuery.replace("'errorMessage'", s"'$escapedError'")
          postgresUtil.insertData(updateTableQuery)
          println(s"[ERROR] Failed to get column ID for '$columnName' in table '$tableName': ${e.getMessage}")
          -1
      }
    }

    val endTime = System.currentTimeMillis()
    val totalTimeSeconds = (endTime - startTime) / 1000
    println(s"Total time taken: $totalTimeSeconds seconds")
    println(s"***************** End of Processing the Mentoring Metabase Dashboard Event with = ${event.tenantCode} *****************")
  }

  private def validateCollection(collectionName: String, reportFor: String, reportId: Option[String] = None): (Boolean, Int) = {
    val mapper = new ObjectMapper()
    println(s">>> Checking Metabase API for collection: $collectionName")
    try {
      val collections = mapper.readTree(metabaseUtil.listCollections())
      val result = collections match {
        case arr: ArrayNode =>
          arr.asScala.find { c =>
              val name = Option(c.get("name")).map(_.asText).getOrElse("")
              val desc = Option(c.get("description")).map(_.asText).getOrElse("")
              val matchesName = name == collectionName
              val matchesReportFor = desc.contains(s"Collection For: $reportFor")
              val isMatch = if (reportId.isEmpty) matchesName && matchesReportFor else matchesName && matchesReportFor
              isMatch
            }.map(c => (true, Option(c.get("id")).map(_.asInt).getOrElse(0)))
            .getOrElse((false, 0))
        case _ => (false, 0)
      }
      println(s">>> API result: $result")
      result
    } catch {
      case e: Exception =>
        println(s"[ERROR] API or JSON failure: ${e.getMessage}")
        (false, 0)
    }
  }
}