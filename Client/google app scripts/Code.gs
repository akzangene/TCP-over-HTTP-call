// Code.gs
const AUTH_TOKEN = "change_me_in_production";   // Must match your Python AUTH_TOKEN

function doPost(e) {
  try {
    // Authentication
    const auth = e.parameter["Authorization"]
    if (!auth || auth !== "Bearer " + AUTH_TOKEN) {
      return HtmlService.createHtmlOutput(JSON.stringify({ e: "unauthorized" }));
    }

    // Target Server
    const targetUrl = e.parameter.targetServer;
    if (!targetUrl) {
      return HtmlService.createHtmlOutput(JSON.stringify({ e: "no_target" }));
    }

    const options = {
      method: "post",
      headers: {
        "Authorization": auth,
        "X-Max-Response-Size": e.parameter["X-Max-Response-Size"]
      },
      payload: e.postData && e.postData.contents,
      muteHttpExceptions: true,
      followRedirects: false,
      escaping: false
    };

    const response = UrlFetchApp.fetch(targetUrl, options);

    // Return structured response like MasterHttpRelayVPN
    const result = {
      s: response.getResponseCode(),           // status
      h: response.getHeaders(),                // headers
      b: response.getContentText()             // body
    };

    return HtmlService.createHtmlOutput(JSON.stringify(result));

  } catch (error) {
    return HtmlService.createHtmlOutput(JSON.stringify({
      e: "relay_error",
      m: error.toString()
    }));
  }
}