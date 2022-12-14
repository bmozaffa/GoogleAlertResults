function fetchRSS() {
  var token = getAirTablePersonalAccessToken();
  var schema = getAirTableSchema();
  var feeds = getFeeds(token, schema);
  var failures = [];
  for (rssFeed of feeds) {
    try {
      var results = getUpdatedResults(rssFeed.url, rssFeed.lastUpdate);
      if (Object.entries(results.entries).length > 0) {
        storeSearchResults(token, schema, rssFeed.recordId, results);
      }
      if (results.timestamp) {
        storeLastUpdate(token, schema, rssFeed.recordId, results.timestamp);
      }
    } catch (err) {
      failures.push(rssFeed);
    }
  }
  if (failures.length === 1) {
    console.error("Failed to get updated Google Alert results for %s with URL %s", failures[0].keywords, failures[0].url);
    throw ("Failed to get all updated Google Alert results");
  } else if (failures.length > 1) {
    console.error("Failed to get updated Google Alert results for %s fees", failures.length);
    for (feed of failures) {
      console.error("Failed to get updated Google Alert results for %s with URL %s", feed.keywords, feed.url);
    }
    throw ("Failed to get all updated Google Alert results");
  }
}

function getFeeds(token, schema) {
  var headers = {};
  headers['Authorization'] = Utilities.formatString('Bearer %s', token);
  var url = Utilities.formatString('https://api.airtable.com/v0/%s/%s', schema.baseId, encodeURIComponent(schema.feeds.table));
  var resp = UrlFetchApp.fetch(url, {
    'headers': headers,
    'method': "GET",
    'muteHttpExceptions': true, // Prevents thrown HTTP exceptions.
  });

  var code = resp.getResponseCode();
  if (code >= 200 && code < 300) {
    var jsonResponse = resp.getContentText("utf-8");
    var records = JSON.parse(jsonResponse).records;
    var feeds = [];
    for (record of records) {
      if (Object.entries(record.fields).length > 0) {
        feed = {
          keywords: record.fields[schema.feeds.keywords],
          url: record.fields[schema.feeds.feedURL],
          recordId: record.id
        };
        var dateString = record.fields[schema.feeds.lastUpdate];
        if (dateString) {
          feed.lastUpdate = new Date(record.fields[schema.feeds.lastUpdate])
        }
        feeds.push(feed);
      }
    }
    return feeds;
  } else if (code === 401 || code === 403) {
    // Not fully authorized for this action.
    throw ("Authorization error: " + code + " with message " + resp.getContentText());
  } else {
    // Handle other response codes by logging them and throwing an
    // exception.
    console.error("Backend server error (%s): %s", code.toString(),
      resp.getContentText("utf-8"));
    throw ("Backend server error: " + code);
  }
}

function storeSearchResults(token, schema, recordId, result) {
  //Split into batches of maximum 10 entries each, as restricted by Airtable:
  for (let i = 0; i < (result.entries.length / 10); i++) {
    let start = i * 10;
    var records = [];
    var keywords = [recordId];
    for (let j = start; j < (start + 10) && j < result.entries.length; j++) {
      records.push({
        fields: {
          [schema.results.title]: result.entries[j].title,
          [schema.results.link]: result.entries[j].link,
          [schema.results.timestamp]: result.entries[j].timestamp.toLocaleDateString(),
          [schema.results.keywords]: keywords
        }
      });
    }
    createRecords(token, schema, records);
  }
}

function createRecords(token, schema, records) {
  var data = {
    records: records
  };
  var headers = {};
  headers['Content-Type'] = 'application/json';
  headers['Authorization'] = Utilities.formatString('Bearer %s', token);
  var url = Utilities.formatString('https://api.airtable.com/v0/%s/%s', schema.baseId, encodeURIComponent(schema.results.table));
  var resp = UrlFetchApp.fetch(url, {
    'headers': headers,
    'method': "POST",
    'muteHttpExceptions': true, // Prevents thrown HTTP exceptions.
    'payload': JSON.stringify(data)
  });
  var code = resp.getResponseCode();
  if (code >= 200 && code < 300) {
    console.log("Stored " + records.length + " new records");
  } else if (code === 401 || code === 403) {
    // Not fully authorized for this action.
    throw ("Authorization error: " + code + " with message " + resp.getContentText());
  } else {
    if (code === 422) {
      for (record of data.records) {
        console.error(record);
      }
    }
    // Handle other response codes by logging them and throwing an
    // exception.
    console.error("Backend server error (%s): %s", code.toString(),
      resp.getContentText("utf-8"));
    throw ("Backend server error: " + code);
  }
}

function getUpdatedResults(url, lastUpdate) {
  var result = {};
  result.entries = [];
  var response = UrlFetchApp.fetch(url).getContentText();
  var root = XmlService.parse(response).getRootElement();
  var namespace = root.getNamespace();
  result.title = root.getChild("title", namespace).getValue();
  timestampElement = root.getChild("updated", namespace);
  if (!timestampElement) {
    console.log("Found RSS feed that has not produced any results yet, likely just created");
    return result;
  }
  result.timestamp = timestampElement.getValue();
  for (entryXML of root.getChildren("entry", namespace)) {
    timestamp = new Date(entryXML.getChild("updated", namespace).getValue());
    if (!lastUpdate || timestamp > lastUpdate) {
      var entry = {};
      entry.title = entryXML.getChild("title", namespace).getValue();
      entry.link = entryXML.getChild("link", namespace).getAttribute("href").getValue();
      entry.timestamp = timestamp;
      result.entries.push(entry);
    }
  }
  console.log("Found " + result.entries.length + " search results out of " + root.getChildren("entry", namespace).length + " total results to be new for " + result.title);
  return result;
}

function storeLastUpdate(token, schema, recordId, timestamp) {
  var data = {
    fields: {
      [schema.feeds.lastUpdate]: timestamp
    }
  };
  var headers = {};
  headers['Content-Type'] = 'application/json';
  headers['Authorization'] = Utilities.formatString('Bearer %s', token);
  var url = Utilities.formatString('https://api.airtable.com/v0/%s/%s/%s', schema.baseId, encodeURIComponent(schema.feeds.table), recordId);
  var resp = UrlFetchApp.fetch(url, {
    'headers': headers,
    'method': "PATCH",
    'muteHttpExceptions': true, // Prevents thrown HTTP exceptions.
    'payload': JSON.stringify(data)
  });
  var code = resp.getResponseCode();
  if (code >= 200 && code < 300) {
    return null;
  } else if (code === 401 || code === 403) {
    // Not fully authorized for this action.
    throw ("Authorization error: " + code + " with message " + resp.getContentText());
  } else {
    if (code === 422) {
      console.error(data);
      if (data.records) {
        for (record of data.records) {
          console.error(record);
        }
      }
    }
    // Handle other response codes by logging them and throwing an
    // exception.
    console.error("Backend server error (%s): %s", code.toString(),
      resp.getContentText("utf-8"));
    throw ("Backend server error: " + code);
  }
}

function getAirTableSchema() {
  return {
    baseId: "app9RtRrw9rTjgbUZ", //appdpBAn1QPm2n9Jw for alerts and app9RtRrw9rTjgbUZ for Press Coverage
    feeds: {
      table: "Google Alerts Keywords",
      keywords: "Alerts Keyword",
      feedURL: "RSS Feed",
      lastUpdate: "Last Query"
    },
    results: {
      table: "Press Coverage",
      timestamp: "Date",
      link: "URL",
      title: "Headline",
      keywords: "Google Alerts Keyword",
    }
  };
}

function getAirTablePersonalAccessToken() {
  return "path4oTeqfmsF14r7.982e7049xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
}
