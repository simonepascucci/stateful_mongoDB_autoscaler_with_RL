/**
 * Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/*
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_database.java
 */
package site.ycsb.db;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MongoDB client for YCSB framework with realistic data generation.
 */
public class MongoDbClient extends DB {
  private static final String DEFAULT_READPREFERENCE = "primary";
  private static final String MONGODB_URL_PROPERTY = "mongodb.url";
  private static final String MONGODB_DB_PROPERTY = "mongodb.database";
  private static final String MONGODB_WRITECONCERN_PROPERTY = "mongodb.writeConcern";
  private static final String MONGODB_READPREFERENCE_PROPERTY = "mongodb.readPreference";
  private static final String MONGODB_MAXCONNECTIONS_PROPERTY = "mongodb.maxconnections";
  private static final String MONGODB_BATCHSIZE_PROPERTY = "mongodb.batchsize";
  private static final String MONGODB_UPSERT_PROPERTY = "mongodb.upsert";
  private static final String MONGODB_QUERY_FIELD = "mongodb.queryfield";

  /** Field names for realistic data generation. */
  private static final String[] FIELD_NAMES = {
      "first_name",
      "last_name",
      "age",
      "email",
      "city",
      "country"
  };

  /** Lists for generating realistic data. */
  private static final String[] FIRST_NAMES = {
      "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth",
      "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
      "Christopher", "Nancy", "Daniel", "Lisa", "Matthew", "Betty", "Anthony", "Margaret", "Donald", "Sandra",
      "Mark", "Ashley", "Paul", "Kimberly", "Steven", "Emily", "Andrew", "Donna", "Kenneth", "Michelle",
      "Joshua", "Dorothy", "George", "Carol", "Kevin", "Amanda", "Brian", "Melissa", "Edward", "Deborah",
      "Ronald", "Stephanie", "Timothy", "Rebecca", "Jason", "Sharon", "Jeffrey", "Laura", "Ryan", "Cynthia",
      "Jacob", "Kathleen", "Gary", "Amy", "Nicholas", "Shirley", "Eric", "Angela", "Stephen", "Helen",
      "Jonathan", "Anna", "Larry", "Brenda", "Justin", "Pamela", "Scott", "Nicole", "Brandon", "Emma",
      "Frank", "Samantha", "Benjamin", "Katherine", "Gregory", "Christine", "Raymond", "Debra", "Samuel", "Rachel",
      "Patrick", "Catherine", "Alexander", "Carolyn", "Jack", "Janet", "Dennis", "Ruth", "Jerry", "Maria"
  };

  private static final String[] LAST_NAMES = {
      "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
      "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
      "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
      "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
      "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts",
      "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker", "Cruz", "Edwards", "Collins", "Reyes",
      "Stewart", "Morris", "Morales", "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper",
      "Peterson", "Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
      "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes",
      "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers", "Long", "Ross", "Foster", "Jimenez"
  };

  private static final String[] CITIES = {
      "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego",
      "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus", "San Francisco", "Charlotte",
      "Indianapolis", "Seattle", "Denver", "Boston", "El Paso", "Nashville", "Detroit", "Oklahoma City",
      "Portland", "Las Vegas", "Memphis", "Louisville", "Baltimore", "Milwaukee", "Albuquerque", "Tucson",
      "Fresno", "Sacramento", "Mesa", "Kansas City", "Atlanta", "Long Beach", "Colorado Springs", "Raleigh",
      "Miami", "Virginia Beach", "Omaha", "Oakland", "Minneapolis", "Tulsa", "Arlington", "New Orleans",
      "Wichita", "Cleveland", "Tampa", "Bakersfield", "Aurora", "Honolulu", "Anaheim", "Santa Ana",
      "Corpus Christi", "Riverside", "Lexington", "St. Louis", "Stockton", "Pittsburgh", "Saint Paul",
      "Cincinnati", "Anchorage", "Henderson", "Greensboro", "Plano", "Newark", "Toledo", "Lincoln",
      "Orlando", "Chula Vista", "Irvine", "Fort Wayne", "Jersey City", "Durham", "St. Petersburg",
      "Laredo", "Buffalo", "Madison", "Lubbock", "Chandler", "Scottsdale", "Glendale", "Reno",
      "Norfolk", "Winston–Salem", "North Las Vegas", "Irving", "Chesapeake", "Gilbert", "Hialeah",
      "Garland", "Fremont", "Richmond", "Boise", "Baton Rouge"
  };

  private static final String[] COUNTRIES = {
      "USA", "Canada", "UK", "France", "Germany", "Spain", 
      "Italy", "Australia", "Japan", "Brazil",
      "Mexico", "India", "China", "Russia", "Sweden", "Norway", 
      "Denmark", "Netherlands", "Switzerland", "Ireland",
      "Belgium", "Austria", "New Zealand", "South Africa", 
      "Argentina", "Chile", "Portugal", "Poland", "Greece", "Turkey",
      "South Korea", "Vietnam", "Thailand", "Indonesia", "Malaysia", 
      "Philippines", "Saudi Arabia", "Libya", "Egypt", "Singapore"
  };


  private MongoClient mongoClient;
  private MongoDatabase database;
  private static final UpdateOptions UPDATE_WITH_UPSERT = new UpdateOptions().upsert(true);
  private static final InsertManyOptions INSERT_UNORDERED = new InsertManyOptions().ordered(false);
  private List<Document> bulkInserts = new ArrayList<>();
  private Random random = new Random();
  private int batchSize;
  private boolean useUpsert;
  private String queryField;

  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /**
   * Initialize MongoDB client.
   */
  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String url = props.getProperty(MONGODB_URL_PROPERTY);
    String dbName = props.getProperty(MONGODB_DB_PROPERTY);
    String writeConcern = props.getProperty(MONGODB_WRITECONCERN_PROPERTY);
    String readPreference = props.getProperty(MONGODB_READPREFERENCE_PROPERTY, DEFAULT_READPREFERENCE);
    String maxConnections = props.getProperty(MONGODB_MAXCONNECTIONS_PROPERTY);
    batchSize = Integer.parseInt(props.getProperty(MONGODB_BATCHSIZE_PROPERTY, "1"));
    useUpsert = Boolean.parseBoolean(props.getProperty(MONGODB_UPSERT_PROPERTY, "false"));
    

    if (url == null || url.isEmpty()) {
      throw new DBException("No MongoDB URL specified");
    }
    if (dbName == null || dbName.isEmpty()) {
      throw new DBException("No MongoDB database specified");
    }

    queryField = props.getProperty(MONGODB_QUERY_FIELD, "country");
    if (!Arrays.asList(FIELD_NAMES).contains(queryField)) {
      throw new DBException("Invalid query field: " + queryField);
    }

    try {
      MongoClientURI uri = new MongoClientURI(url);
      mongoClient = new MongoClient(uri);
      database = mongoClient.getDatabase(dbName).withReadPreference(ReadPreference.valueOf(readPreference));
      if (writeConcern != null) {
        database = database.withWriteConcern(WriteConcern.valueOf(writeConcern));
      }
    } catch (Exception e) {
      throw new DBException("Could not initialize MongoDB client", e);
    }

    INIT_COUNT.incrementAndGet();
  }

  /**
   * Cleanup MongoDB client resources.
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0 && mongoClient != null) {
      mongoClient.close();
    }
  }

  /**
   * Generate realistic data based on field name.
   */
  private String generateRealisticData(String fieldName) {
    String firstName = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
    String lastName = LAST_NAMES[random.nextInt(LAST_NAMES.length)];
    switch (fieldName) {
    case "first_name":
      return firstName;
    case "last_name":
      return lastName;
    case "age":
      return String.valueOf(18 + random.nextInt(82)); // ages 18-99
    case "email":
      return firstName + "." + lastName + "@example.com";
    case "city":
      return CITIES[random.nextInt(CITIES.length)];
    case "country":
      return COUNTRIES[random.nextInt(COUNTRIES.length)];
    default:
      return "unknown";
    }
  }

  /**
   * Insert a record with realistic data.
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document toInsert = new Document("_id", key);
      
      // Generate realistic data for each field
      for (String fieldName : FIELD_NAMES) {
        String value = generateRealisticData(fieldName);
        toInsert.put(fieldName, value);
      }

      if (batchSize == 1) {
        if (useUpsert) {
          collection.replaceOne(new Document("_id", toInsert.get("_id")), toInsert, UPDATE_WITH_UPSERT);
        } else {
          collection.insertOne(toInsert);
        }
      } else {
        bulkInserts.add(toInsert);
        if (bulkInserts.size() == batchSize) {
          if (useUpsert) {
            List<UpdateOneModel<Document>> updates = new ArrayList<>(bulkInserts.size());
            for (Document doc : bulkInserts) {
              updates.add(new UpdateOneModel<>(
                  new Document("_id", doc.get("_id")),
                  doc, UPDATE_WITH_UPSERT));
            }
            collection.bulkWrite(updates);
          } else {
            collection.insertMany(bulkInserts, INSERT_UNORDERED);
          }
          bulkInserts.clear();
        } else {
          return Status.BATCHED_OK;
        }
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with " + bulkInserts.size());
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Read a record.
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                    Map<String, ByteIterator> result) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      
      // Generate and log query value
      String queryValue = generateRealisticData(queryField);
      Document query = new Document(queryField, queryValue);
      
      // Log every 1000th query for analysis
      if (random.nextInt(1000) == 0) {
        System.out.printf("\n[DEBUG] Query Details:\n" +
            "- Query Field: %s\n" +
            "- Query Value: %s\n" +
            "- Collection: %s\n" +
            "- Database: %s\n",
            queryField, queryValue, table, database.getName());

        // Get explain plan using runCommand
        Document explainCmd = new Document("explain",
            new Document("find", table)
                .append("filter", query));
        Document explainPlan = database.runCommand(explainCmd);
        System.out.println("- Explain Plan: " + explainPlan.toJson());
        
        // Get collection stats
        Document collStats = database.runCommand(new Document("collStats", table));
        System.out.printf("- Collection Stats:\n" +
            "  * Sharded: %s\n" +
            "  * Chunks: %s\n" +
            "  * Documents: %d\n",
            collStats.get("sharded", false),
            collStats.get("chunks", "N/A"),
            collStats.get("count", 0));
      }
      
      Document projection = null;
      if (fields != null) {
        projection = new Document();
        for (String field : fields) {
          projection.put(field, 1);
        }
      }

      long startTime = System.nanoTime();
      Document doc = collection.find(query).projection(projection).first();
      long endTime = System.nanoTime();

      // Log slow queries (over 100ms)
      long queryTimeMs = (endTime - startTime) / 1_000_000;
      if (queryTimeMs > 100) {
        System.out.printf("[SLOW QUERY] %dms - Field: %s, Value: %s\n",
            queryTimeMs, queryField, queryValue);
      }

      if (doc != null) {
        fillMap(result, doc);
        return Status.OK;
      }
      return Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println("Query failed: " + e.toString());
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Update a record with realistic data.
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document query = new Document("_id", key);
      Document fieldsToSet = new Document();
      
      // Generate new realistic data for each field
      for (String fieldName : FIELD_NAMES) {
        String value = generateRealisticData(fieldName);
        fieldsToSet.put(fieldName, value);
      }
      
      Document update = new Document("$set", fieldsToSet);
      UpdateResult result = collection.updateOne(query, update);
      
      if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
        System.err.println("Nothing updated for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Delete a record.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document query = new Document("_id", key);
      collection.deleteOne(query);
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document query = new Document("_id", new Document("$gte", startkey));
      Document projection = null;
      if (fields != null) {
        projection = new Document();
        for (String field : fields) {
          projection.put(field, 1);
        }
      }

      MongoCursor<Document> cursor = collection.find(query)
          .projection(projection)
          .limit(recordcount)
          .iterator();

      while (cursor.hasNext()) {
        Document doc = cursor.next();
        HashMap<String, ByteIterator> resultMap = new HashMap<>();
        fillMap(resultMap, doc);
        result.add(resultMap);
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Helper method to fill result map from MongoDB document.
   */
  private void fillMap(Map<String, ByteIterator> result, Document doc) {
    for (String key : doc.keySet()) {
      if (key.equals("_id")) {
        continue;
      }
      Object value = doc.get(key);
      if (value != null) {
        result.put(key, new StringByteIterator(value.toString()));
      }
    }
  }
}
