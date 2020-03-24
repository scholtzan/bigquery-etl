
CREATE TEMPORARY FUNCTION get_client_ip(
  xff STRING, 
  remote_address STRING,
  pipeline_proxy STRING
)
RETURNS STRING
LANGUAGE js
AS
  """
  if (xff != null) {
    // Google's load balancer will append the immediate sending client IP and a global
    // forwarding rule IP to any existing content in X-Forwarded-For as documented in:
    // https://cloud.google.com/load-balancing/docs/https/#components
    //
    // In practice, many of the "first" addresses are bogus or internal,
    // so we target the immediate sending client IP.
    ips = xff.split(",");

    if (pipeline_proxy != null) {
      ips = ips.slice(0, -1);
    }

    ip = ips[Math.max(ips.length - 2, 0)].trim();
  } else {
    ip = "";
    if (remote_address != null) {
      ip = remote_address;
    }
  }

  return ip;
  """;
CREATE TEMPORARY FUNCTION cidr_range(
  network STRING
)
RETURNS STRUCT<
  start_ip FLOAT64,
  end_ip FLOAT64>
LANGUAGE js
AS
  """
  function ipToInt(ipAddress) 
  {
    var d = ipAddress.split('.');
    return BigInt(((((((+d[0])*256)+(+d[1]))*256)+(+d[2]))*256)+(+d[3]));
  }

  function ipMask(maskSize) {
    return -1n << (32n - BigInt(parseInt(maskSize)));
  }

  var subnetIp = network.split("/")[0];
  var subnetMask = network.split("/")[1];
  var startIp = ipToInt(subnetIp) & ipMask(subnetMask);
  var endIp = startIp + BigInt(Math.pow(2, (32 - subnetMask))) - 1n;


  return {
    "start_ip": parseFloat(startIp),
    "end_ip": parseFloat(endIp)
  };
  """;
--
WITH 
asn_ip_address_range AS (
  SELECT 
    NET.IPV4_FROM_INT64(CAST(cidr_range(network).start_ip AS INT64)) as start_ip,
    NET.IPV4_FROM_INT64(CAST(cidr_range(network).end_ip AS INT64)) as end_ip,
    NET.IP_TRUNC(NET.IPV4_FROM_INT64(CAST(cidr_range(network).start_ip AS INT64)), CAST(SPLIT(network, "/")[OFFSET(1)] AS INT64)) as prefix,
    autonomous_system_number
  FROM 
    `static.geoip2_isp_blocks_ipv4`
  ORDER BY start_ip ASC
),
main_summary_with_ip AS (
  SELECT 
    submission_date,
    client_id,
    user_pref_network_trr_mode as trr,
    NET.SAFE_IP_FROM_STRING(ip_address) as ip_address
  FROM `moz-fx-data-shared-prod.telemetry.main_summary` as main_summary
  LEFT JOIN
    (SELECT 
      submission_timestamp,
      get_client_ip(x_forwarded_for, remote_addr, x_pipeline_proxy) as ip_address,
      udf.parse_desktop_telemetry_uri(uri).document_id AS document_id
    FROM `moz-fx-data-shared-prod.payload_bytes_raw.telemetry`) AS payload_bytes_raw
  ON payload_bytes_raw.document_id = main_summary.document_id
  WHERE submission_date = '2020-03-20' AND
  DATE(payload_bytes_raw.submission_timestamp) = '2020-03-20'
),
main_summary_with_asn AS (
  SELECT 
    submission_date,
    client_id,
    trr,
    autonomous_system_number,
  FROM main_summary_with_ip
  JOIN asn_ip_address_range AS a
  ON a.prefix = NET.IP_TRUNC(ip_address, 16)
    WHERE ip_address BETWEEN a.start_ip AND a.end_ip
)
SELECT 
  submission_date,
  autonomous_system_number,
  COUNT(distinct client_id) AS n_clients,
  COUNTIF(trr > 0) as doh_enabled,
  COUNTIF(trr = 0) as doh_disabled
FROM main_summary_with_asn
GROUP BY 
  submission_date, 
  autonomous_system_number
HAVING n_clients  > 50