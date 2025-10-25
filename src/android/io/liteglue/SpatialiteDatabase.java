/*
 * Copyright (c) 2025, Adrien THO @ Ciril GROUP
 * Copyright (c) 2012-2015, Chris Brody
 * Copyright (c) 2005-2010, Nitobi Software Inc.
 * Copyright (c) 2010, IBM Corporation
 */

package io.liteglue;

import android.annotation.SuppressLint;
import android.util.Log;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.SQLException;
import spatialite.database.SQLiteDatabase;
import spatialite.database.SQLiteStatement;

import org.apache.cordova.CallbackContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Spatialite Database helper class adapted for android_spatialite JNI.
 */
class SpatialiteDatabase {

    private static final Pattern FIRST_WORD = Pattern.compile("^\\s*(\\S+)", Pattern.CASE_INSENSITIVE);

    private SQLiteDatabase db;

    /**
     * Open a database.
     */
    void open(File dbfile) throws Exception {
        if (!dbfile.exists()) {
            Log.d(SpatialiteDatabase.class.getSimpleName(), "Creating sqlite db: " + dbfile.getAbsolutePath());
        }

        Log.d(SpatialiteDatabase.class.getSimpleName(), "Open sqlite db: " + dbfile.getAbsolutePath());
        db = SQLiteDatabase.openDatabase(dbfile.getAbsolutePath(), null,
                SQLiteDatabase.OPEN_READWRITE | SQLiteDatabase.CREATE_IF_NECESSARY);
    }

    /**
     * Close a database.
     */
    void closeDatabaseNow() {
        if (db != null && db.isOpen()) {
            try {
                db.close();
                Log.d(SpatialiteDatabase.class.getSimpleName(), "Database closed");
            } catch (Exception e) {
                e.printStackTrace();
                Log.d(SpatialiteDatabase.class.getSimpleName(), "closeDatabaseNow(): Error=" + e.getMessage());
            }
            db = null;
        }
    }

    /**
     * Executes a batch request and sends the results via cbc.
     */
    @SuppressLint("NewApi")
    void executeSqlBatch(String[] queryArr, JSONArray[] jsonParams, String[] queryIDs, CallbackContext cbc) {
        if (db == null || !db.isOpen()) {
            cbc.error("database has been closed");
            return;
        }

        JSONArray batchResults = new JSONArray();

        for (int i = 0; i < queryArr.length; i++) {
            String queryId = queryIDs[i];
            JSONObject queryResult = executeQuery(queryArr, jsonParams, i);

            try {
                if (queryResult == null || queryResult.has("error")) {
                    JSONObject r = new JSONObject();
                    r.put("qid", queryId);
                    r.put("type", "error");
                    JSONObject er = new JSONObject();
                    er.put("message", queryResult != null ? queryResult.optString("error", "unknown") : "unknown");
                    r.put("result", er);
                    batchResults.put(r);
                } else {
                    JSONObject r = new JSONObject();
                    r.put("qid", queryId);
                    r.put("type", "success");
                    r.put("result", queryResult);
                    batchResults.put(r);
                }
            } catch (JSONException ex) {
                ex.printStackTrace();
                Log.e("executeSqlBatch", "Error creating batch result: " + ex.getMessage(), ex);
            }
        }

        cbc.success(batchResults);
    }

    /**
     * Executes a single query.
     */
    private JSONObject executeQuery(String[] queryArr, JSONArray[] jsonParams, int i) {
        JSONObject result = new JSONObject();
        try {
            String query = queryArr[i];
            QueryType queryType = getQueryType(query);

            switch (queryType) {
                case update:
                case delete:
                    db.execSQL(query, getArgs(jsonParams[i]));
                    result.put("rowsAffected", db.changedRowCount());
                    break;

                case insert:
                    db.execSQL(query, getArgs(jsonParams[i]));
                    result.put("insertId", db.compileStatement("SELECT last_insert_rowid()").simpleQueryForLong());
                    result.put("rowsAffected", db.changedRowCount());
                    break;

                case begin:
                    db.beginTransaction();
                    result = new JSONObject();
                    break;

                case commit:
                    db.setTransactionSuccessful();
                    db.endTransaction();
                    result = new JSONObject();
                    break;

                case rollback:
                    db.endTransaction();
                    result = new JSONObject();
                    break;

                default:
                    result = executeRawQuery(query, jsonParams[i]);
                    break;
            }
        } catch (Exception ex) {
            try {
                result.put("error", ex.getMessage());
            } catch (JSONException ignore) {
            }
        }
        return result;
    }

    private JSONObject executeRawQuery(String query, JSONArray params) throws JSONException {
        JSONObject result = new JSONObject();
        JSONArray rows = new JSONArray();
        Cursor cursor = db.rawQuery(query, getArgs(params));

        if (cursor != null) {
            try {
                while (cursor.moveToNext()) {
                    JSONObject row = new JSONObject();
                    for (int j = 0; j < cursor.getColumnCount(); j++) {
                        String columnName = cursor.getColumnName(j);

                        if (cursor.isNull(j)) {
                            row.put(columnName, JSONObject.NULL);
                        } else {
                            int type = cursor.getType(j);
                            switch (type) {
                                case Cursor.FIELD_TYPE_INTEGER:
                                    row.put(columnName, cursor.getLong(j));
                                    break;
                                case Cursor.FIELD_TYPE_FLOAT:
                                    row.put(columnName, cursor.getDouble(j));
                                    break;
                                case Cursor.FIELD_TYPE_BLOB:
                                    row.put(columnName, cursor.getBlob(j));
                                    break;
                                case Cursor.FIELD_TYPE_STRING:
                                default:
                                    row.put(columnName, cursor.getString(j));
                                    break;
                            }
                        }
                    }
                    rows.put(row);
                }
            } finally {
                cursor.close();
            }
        }

        result.put("rows", rows);
        return result;
    }

    private String[] getArgs(JSONArray params) throws JSONException {
        if (params == null) return null;
        String[] args = new String[params.length()];
        for (int i = 0; i < params.length(); i++) {
            args[i] = params.getString(i);
        }
        return args;
    }

    private QueryType getQueryType(CharSequence query) {
        Matcher matcher = FIRST_WORD.matcher(query);
        if (matcher.find()) {
            try {
                return QueryType.valueOf(matcher.group(1).toLowerCase());
            } catch (IllegalArgumentException ignore) {
            }
        }
        return QueryType.other;
    }

    private enum QueryType {
        update, insert, delete, select, begin, commit, rollback, other
    }
} /* vim: set expandtab : */
