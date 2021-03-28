package com.itheima.bean

case class PageViewsBeanCase(session: String,
                             remote_addr: String,
                             time_local: String,
                             request: String,
                             visit_step: Int,
                             page_staylong: Long,
                             htp_referer: String,
                             http_user_agent: String,
                             body_bytes_send: String,
                             status: String,
                             guid: String)
