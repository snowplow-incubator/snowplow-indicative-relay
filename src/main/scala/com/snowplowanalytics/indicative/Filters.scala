/*
 * Copyright (c) 2018-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.indicative

object Filters {

  val unusedEvents: List[String] = List(
    "app_heartbeat",
    "app_initialized",
    "app_shutdown",
    "app_warning",
    "create_event",
    "emr_job_failed",
    "emr_job_started",
    "emr_job_status",
    "emr_job_succeeded",
    "incident",
    "incident_assign",
    "incident_notify_of_close",
    "incident_notify_user",
    "job_update",
    "load_failed",
    "load_succeeded",
    "page_ping",
    "s3_notification_event",
    "send_email",
    "send_message",
    "storage_write_failed",
    "stream_write_failed",
    "task_update",
    "wd_access_log"
  )

  val unusedAtomicFields: List[String] = List(
    "etl_tstamp",
    "collector_tstamp",
    "dvce_created_tstamp",
    "event",
    "txn_id",
    "name_tracker",
    "v_tracker",
    "v_collector",
    "v_etl",
    "user_fingerprint",
    "geo_latitude",
    "geo_longitude",
    "ip_isp",
    "ip_organization",
    "ip_domain",
    "ip_netspeed",
    "page_urlscheme",
    "page_urlport",
    "page_urlquery",
    "page_urlfragment",
    "refr_urlscheme",
    "refr_urlport",
    "refr_urlquery",
    "refr_urlfragment",
    "pp_xoffset_min",
    "pp_xoffset_max",
    "pp_yoffset_min",
    "pp_yoffset_max",
    "br_features_pdf",
    "br_features_flash",
    "br_features_java",
    "br_features_director",
    "br_features_quicktime",
    "br_features_realplayer",
    "br_features_windowsmedia",
    "br_features_gears",
    "br_features_silverlight",
    "br_cookies",
    "br_colordepth",
    "br_viewwidth",
    "br_viewheight",
    "dvce_ismobile",
    "dvce_screenwidth",
    "dvce_screenheight",
    "doc_charset",
    "doc_width",
    "doc_height",
    "tr_currency",
    "mkt_clickid",
    "etl_tags",
    "dvce_sent_tstamp",
    "refr_domain_userid",
    "refr_device_tstamp",
    "derived_tstamp",
    "event_vendor",
    "event_name",
    "event_format",
    "event_version",
    "event_fingerprint",
    "true_tstamp"
  )

  val unusedContexts: List[String] = List(
    "application_context",
    "application_error",
    "duplicate",
    "geolocation_context",
    "instance_identity_document",
    "java_context",
    "jobflow_step_status",
    "parent_event",
    "performance_timing",
    "timing"
  )

}
