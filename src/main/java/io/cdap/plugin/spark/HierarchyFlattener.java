/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.spark;

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.broadcast;

/**
 * Takes an RDD that represents a hierarchical structure and flattens it.
 * <p>
 * The input RDD is expected to edges in a tree, with each record containing an id for that node and the id of its
 * parent. The flattened RDD will contain rows denoting each node -> accessible child relationship, along with
 * additional information about that relationship. For example, if the input is:
 * <p>
 * parent:null, id:A  (A as root)
 * parent:A, id:B     (A -> B)
 * parent:B, id:C     (B -> C)
 * parent:B, id:D     (B -> D)
 * <p>
 * The output will be:
 * <p>
 * parent:A, id:A, depth:0, isTop:true, isBot:false
 * parent:A, id:B, depth:1, isTop:false, isBot:false
 * parent:A, id:C, depth:2, isTop:false, isBot:true
 * parent:A, id:D, depth:2, isTop:false, isBot:true
 * parent:B, id:B, depth:0, isTop:false, isBot:false
 * parent:B, id:C, depth:1, isTop:false, isBot:true
 * parent:B, id:D, depth:1, isTop:false, isBot:true
 * parent:C, id:C, depth:0, isTop:false, isBot:true
 * parent:D, id:D, depth:0, isTop:false, isBot:true
 */
public class HierarchyFlattener {
  private static final Logger LOG = LoggerFactory.getLogger(HierarchyFlattener.class);
  private static final boolean SHOW_DEBUG_CONTENT = false;
  private static final boolean SHOW_DEBUG_COLUMNS = false;
  private static final boolean SHOW_DEBUG_INPUT = false;

  private final Map<String, Object> parentRootValues = new HashMap<>();
  private final Map<String, Object> childRootValues = new HashMap<>();
  private final Map<String, String> parentChildMapping;
  private final List<AbstractMap.SimpleImmutableEntry<String, String>> parentChildMappingAsList;

  private final List<Map<String, String>> pathFields;
  private final List<Map<String, String>> connectByRootFields;

  private final String levelCol;
  private final String topCol;
  private final String botCol;

  private final String trueStr;
  private final String falseStr;

  private final int maxLevel;
  private final Boolean broadcastJoin;
  private final String siblingOrder;
  private final List<String> startWithConditions;
  private Schema inputSchema;
  private Column startWithColumn;

  public HierarchyFlattener(HierarchyConfig config) {
    this.levelCol = config.getLevelField();
    this.topCol = config.getTopField();
    this.botCol = config.getBottomField();
    this.trueStr = config.getTrueValue();
    this.falseStr = config.getFalseValue();
    this.maxLevel = config.getMaxDepth();
    this.broadcastJoin = config.isBroadcastJoin();
    this.parentChildMapping = config.getParentChildMapping();
    this.parentChildMappingAsList = config.getParentChildMappingAsList();
    this.pathFields = config.getPathFields();
    this.connectByRootFields = config.getConnectByRootFields();
    this.siblingOrder = config.getSiblingOrder();
    this.startWithConditions = config.getStartWithConditions();

    if (SHOW_DEBUG_INPUT) {
      LOG.info("=================================");
      LOG.info("== startWithConditions - BEGIN ==");
      LOG.info("=================================");
      for (String condition : startWithConditions) {
        LOG.info("== " + condition);
      }
      LOG.info("===============================");
      LOG.info("== startWithConditions - END ==");
      LOG.info("===============================");
    }

    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("====================================");
      LOG.info("== pathFields before parsing: " + config.getRawPathFields() + " ==");
      LOG.info("====================================");

      LOG.info("======================================");
      LOG.info("== pathFields after parsing - BEGIN ==");
      LOG.info("======================================");
      for (Map<String, String> map : this.pathFields) {
        for (String k : map.keySet()) {
          LOG.info("== " + k + ":" + map.get(k));
        }
      }
      LOG.info("====================================");
      LOG.info("== pathFields after parsing - END ==");
      LOG.info("====================================");

      LOG.info("================================================");
      LOG.info("=== connectByRootFields after parsing - BEGIN ==");
      LOG.info("================================================");
      for (Map<String, String> map : this.connectByRootFields) {
        for (String k : map.keySet()) {
          LOG.info("== " + k + ":" + map.get(k));
        }
      }
      LOG.info("=============================================");
      LOG.info("== connectByRootFields after parsing - END ==");
      LOG.info("=============================================");

      LOG.info("==============================================");
      LOG.info("== parentChildMapping after parsing - BEGIN ==");
      LOG.info("==============================================");
      for (Map.Entry<String, String> mapping : parentChildMapping.entrySet()) {
        LOG.info("== " + mapping.getKey() + ":" + mapping.getValue());
      }
      LOG.info("============================================");
      LOG.info("== parentChildMapping after parsing - END ==");
      LOG.info("============================================");
    }
  }

  /**
   * Takes an input RDD and flattens it so that every possible ancestor to child relationship is present in the output.
   * Each output record also is annotated with whether the ancestor is a root node, whether the child is a leaf node,
   * and the distance from the ancestor to the child.
   * <p>
   * Suppose the input data represents the hierarchy
   * <p>
   * |--> 5
   * |--> 2 --|
   * 1 --|        |--|
   * |--> 3      |--> 6
   * 4 --|
   * <p>
   * This would be represented with the following rows:
   * <p>
   * [1 -> 1]
   * [1 -> 2]
   * [1 -> 3]
   * [2 -> 5]
   * [2 -> 6]
   * [4 -> 4]
   * [4 -> 6]
   * <p>
   * and would generate the following output:
   * <p>
   * [1 -> 1, level:0, root:yes, leaf:no]
   * [1 -> 2, level:1, root:yes, leaf:no]
   * [1 -> 3, level:1, root:yes, leaf:no]
   * [1 -> 5, level:2, root:yes, leaf:yes]
   * [1 -> 6, level:2, root:yes, leaf:yes]
   * [2 -> 2, level:0, root:no, leaf:no]
   * [2 -> 5, level:1, root:no, leaf:yes]
   * [2 -> 6, level:1, root:no, leaf:yes]
   * [3 -> 3, level:0, root:no, leaf:yes]
   * [4 -> 4, level:0, root:yes, leaf:no]
   * [4 -> 6, level:1, root:yes, leaf:yes]
   * [5 -> 5, level:0, root:no, leaf:yes]
   * [6 -> 6, level:0, root:no, leaf:yes]
   *
   * @param context spark plugin context
   * @param rdd     input rdd representing a hierarchy
   * @return flattened hierarchy with level, root, and leaf information
   */
  public JavaRDD<StructuredRecord> flatten(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> rdd,
                                           Schema outputSchema) {
    inputSchema = context.getInputSchema();
    SQLContext sqlContext = new SQLContext(context.getSparkContext());
    StructType sparkSchema = DataFrames.toDataType(inputSchema);

    Dataset<Row> input = sqlContext.createDataFrame(
        rdd.map((StructuredRecord record) -> DataFrames.toRow(record, sparkSchema)).rdd(),
        sparkSchema);

    if (SHOW_DEBUG_CONTENT) {
      LOG.info("==============================");
      LOG.info("== Content of input - BEGIN ==");
      LOG.info("==============================");
      for (Iterator<Row> it = input.javaRDD().toLocalIterator(); it.hasNext(); ) {
        Row line = it.next();
        LOG.info("== " + line);
      }
      LOG.info("============================");
      LOG.info("== Content of input - END ==");
      LOG.info("=============================");
    }

    // Cache the input so that the previous stages don't get re-processed
    input = input.persist(StorageLevel.DISK_ONLY());
    // Field names without the parent and child fields.
    List<String> dataFieldNames = inputSchema.getFields().stream()
        .map(Schema.Field::getName)
        .filter(name -> isNotInParentChildMapping(parentChildMapping, name))
        .collect(Collectors.toList());

    /*
       The approach is to take N passes through the hierarchy, where N is the maximum depth of the tree.
       In iteration i, paths of length i - 1 are found.

       The first step is to generate the 0 distance paths.

       With input rows:

         [parent:1, child:1, category:grocery]
         [parent:1, child:2, category:vegetable]
         [parent:1, child:3, category:dairy]
         [parent:2, child:5, category:lettuce]
         [parent:2, child:6, category:tomato]
         [parent:4, child:4, category:fruit]
         [parent:4, child:6, category:tomato]

       the starting points are:

         [parent:1, child:1, level:0, root:true, leaf:0, category:grocery]
         [parent:2, child:2, level:0, root:false, leaf:0, category:vegetable]
         [parent:3, child:3, level:0, root:false, leaf:0, category:dairy]
         [parent:4, child:4, level:0, root:true, leaf:0, category:fruit]
         [parent:5, child:5, level:0, root:false, leaf:0, category:lettuce]
         [parent:6, child:6, level:0, root:false, leaf:0, category:tomato]
     */

    if (SHOW_DEBUG_CONTENT) {
      LOG.info("======================================");
      LOG.info("== Starting computation for level 0 ==");
      LOG.info("======================================");
    }
    Dataset<Row> currentLevel = getStartingPoints(input, dataFieldNames);
    Dataset<Row> flattened = currentLevel;

    if (SHOW_DEBUG_CONTENT) {
      LOG.info("=====================================");
      LOG.info("== content of currentLevel - BEGIN ==");
      LOG.info("=====================================");
      for (Iterator<Row> it = currentLevel.javaRDD().toLocalIterator(); it.hasNext(); ) {
        Row line = it.next();
        LOG.info("== " + line);
      }
      LOG.info("===================================");
      LOG.info("== content of currentLevel - END ==");
      LOG.info("===================================");
    }

    /*
       Each time through the loop, we find paths for the next level and identify leaf rows for the current level
       This is done by performing a left outer join on the current level with the original input.

         select
           current.parent as parent,
           input.child == null ? current.child as child,
           current.level + 1 as level,
           current.root as root,
           input.child == null ? true : false as leaf,
           input.datafield1, input.datafield2, ..., input.datafieldN
         from current left outer join input on current.child = input.parent
         where parent <> child or leaf = 1

       For example, the first time through, currentLevel is:

         [parent:1, child:1, level:0, root:true, leaf:0, category:grocery]
         [parent:2, child:2, level:0, root:false, leaf:0, category:vegetable]
         [parent:3, child:3, level:0, root:false, leaf:0, category:dairy]
         [parent:4, child:4, level:0, root:true, leaf:0, category:fruit]
         [parent:5, child:5, level:0, root:false, leaf:0, category:lettuce]
         [parent:6, child:6, level:0, root:false, leaf:0, category:tomato]

       When joined, this produces:

         [parent:1, child:2, level:1, root:true, leaf:0, category:vegetable]
         [parent:1, child:3, level:1, root:true, leaf:0, category:dairy]
         [parent:2, child:5, level:1, root:false, leaf:0, category:lettuce]
         [parent:2, child:6, level:1, root:false, leaf:0, category:tomato]
         [parent:3, child:3, level:0, root:false, leaf:1, category:dairy]
         [parent:4, child:6, level:1, root:true, leaf:0, category:tomato]
         [parent:5, child:5, level:0, root:false, leaf:1, category:lettuce]
         [parent:6, child:6, level:0, root:false, leaf:1, category:tomato]

       These result get added to the flattened output. All non-leaf rows become the next currentLevel.
       This continues until the max depth is reach, or until there are no more non-leaf rows.
     */
    int level = 0;
    while (!isEmpty(currentLevel)) {
      if (level > maxLevel) {
        throw new IllegalStateException(
            String.format("Exceeded maximum depth of %d. " +
                "Ensure there are no cycles in the hierarchy, or increase the max depth.", maxLevel));
      }

      if (SHOW_DEBUG_CONTENT) {
        LOG.info("=======================================");
        LOG.info("== Starting computation for level {} ==", level);
        LOG.info("=======================================");
      }
      /*
         select
           current.parent as parent,
           input.child == null ? current.child : input.child as child,
           current.level + 1 as level,
           false as root,
           input.child == null ? 1 : 0 as leaf,
           input.child == null ? current.datafield1 : input.datafield1,
           ...,
           input.child == null ? current.datafieldN : input.datafieldN
         from current left outer join input on current.child = input.parent
       */

      // 2 * pathFields.size() to account for 2 extra columns for the path & path length
      // 1 * connectByRootFields.size() to account for 1 extra column for the connect_by_root
      Column[] columns = new Column[2 * parentChildMapping.size()
          + 3
          + dataFieldNames.size()
          + 2 * pathFields.size()
          + connectByRootFields.size()];
      // currentLevel is aliased as "current" and input is aliased as "input"
      // to remove ambiguity between common column names.
      int i = 0;
      Column accColumn = null;
      for (Map.Entry<String, String> map : parentChildMapping.entrySet()) {
        columns[i++] = new Column("current." + map.getKey()).as(map.getKey());
        columns[i++] = functions.when(new Column("input." + map.getValue()).isNull(),
            new Column("current." + map.getValue()))
            .otherwise(new Column("input." + map.getValue())).as(map.getValue());
        if (accColumn == null) {
          accColumn = new Column("input." + map.getValue()).isNull();
        } else {
          accColumn = accColumn.and(new Column("input." + map.getValue()).isNull());
        }
      }
      columns[i++] = new Column(levelCol).plus(1).as(levelCol);
      columns[i++] = functions.lit(falseStr).as(topCol);
      columns[i++] = functions.when(accColumn, 1).otherwise(0).as(botCol);

      for (String fieldName : dataFieldNames) {
        if (parentChildMapping.containsKey(fieldName)) {
          // if there is a field mapping, this means the data field is an attribute of the parent and not of the child.
          // that means it should be taken from the current side and not the input side.
          columns[i++] = new Column("current." + fieldName);
        } else {
          Column whenIsNull = null;
          for (Map.Entry<String, String> map : parentChildMapping.entrySet()) {
            if (whenIsNull == null) {
              whenIsNull = new Column("input." + map.getValue()).isNull();
            } else {
              whenIsNull = whenIsNull.and(new Column("input." + map.getValue()).isNull());
            }
          }
          columns[i++] = functions.when(whenIsNull,
              new Column("current." + fieldName))
              .otherwise(new Column("input." + fieldName)).as(fieldName);
        }
      }

      // Path & Path_length columns
      for (Map<String, String> pathField : pathFields) {
        columns[i++] = functions.concat(
            new Column(pathField.get(HierarchyConfig.PATH_FIELD_ALIAS)),
            functions.lit(pathField.get(HierarchyConfig.PATH_SEPARATOR)),
            new Column("input." + pathField.get(HierarchyConfig.VERTEX_FIELD_NAME))
        ).as(pathField.get(HierarchyConfig.PATH_FIELD_ALIAS));
        columns[i++] = new Column(pathField.get(HierarchyConfig.PATH_FIELD_LENGTH_ALIAS)).plus(1)
            .as(pathField.get(HierarchyConfig.PATH_FIELD_LENGTH_ALIAS));
      }

      // CONNECT_BY_ROOT column
      for (Map<String, String> cbrField : connectByRootFields) {
        columns[i++] = new Column(cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_ALIAS))
            .as(cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_ALIAS));
      }

      if (SHOW_DEBUG_COLUMNS) {
        LOG.info("===========================================");
        LOG.info("== currentLevel:columns - BEGIN - Level {} ==", level);
        LOG.info("===========================================");
        for (Column col : columns) {
          if (col != null) {
            LOG.info("== " + col);
          }
        }
        LOG.info("================================");
        LOG.info("== currentLevel:columns - END ==");
        LOG.info("================================");
      }

      Dataset<Row> nextLevel;
      Column joinColumn = null;
      for (Map.Entry<String, String> map : parentChildMapping.entrySet()) {
        if (joinColumn == null) {
          joinColumn = new Column("current." + map.getValue()).equalTo(new Column("input." + map.getKey()));

        } else {
          joinColumn = joinColumn
              .and(new Column("current." + map.getValue()).equalTo(new Column("input." + map.getKey())));
        }
      }

      if (broadcastJoin) {
        nextLevel = currentLevel.alias("current")
            .join(broadcast(input.alias("input")),
                joinColumn,
                "leftouter")
            .select(columns);
      } else {
        nextLevel = currentLevel.alias("current")
            .join(input.alias("input"),
                joinColumn,
                "leftouter")
            .select(columns);
      }

      if (SHOW_DEBUG_CONTENT) {
        LOG.info("===============================================");
        LOG.info("== Content of currentLevel - Begin - Level {} ==", level);
        LOG.info("===============================================");
        for (Iterator<Row> it = currentLevel.javaRDD().toLocalIterator(); it.hasNext(); ) {
          Row line = it.next();
          LOG.info("== " + line);
        }
        LOG.info("=====================================");
        LOG.info("== Content of currentLevel - END   ==");
        LOG.info("=====================================");
        LOG.info("== Joined with:");
        LOG.info("==============================");
        LOG.info("== Content of input - BEGIN - Level {} ==", level);
        LOG.info("==============================");
        for (Iterator<Row> it = input.javaRDD().toLocalIterator(); it.hasNext(); ) {
          Row line = it.next();
          LOG.info("== " + line);
        }
        LOG.info("==============================");
        LOG.info("== Content of input - END   ==");
        LOG.info("==============================");
      }

      if (SHOW_DEBUG_COLUMNS) {
        LOG.info("========================");
        LOG.info("== joinColumn - BEGIN  - Level {} ==", level);
        LOG.info("========================");
        LOG.info("== " + joinColumn);
        LOG.info("======================");
        LOG.info("== joinColumn - END ==");
        LOG.info("======================");

        LOG.info("=============================");
        LOG.info("== selected columns - BEGIN - Level {} ==", level);
        LOG.info("=============================");
        for (Column col : columns) {
          LOG.info("== " + col);
        }
        LOG.info("============================");
        LOG.info("== selected columns - END ==");
        LOG.info("============================");
      }

      if (SHOW_DEBUG_CONTENT) {
        LOG.info("===============================================================================");
        LOG.info("== Content of nextLevel after join, before where - BEGIN - Level {} ==", level);
        LOG.info("===============================================================================");
        for (Iterator<Row> it = nextLevel.javaRDD().toLocalIterator(); it.hasNext(); ) {
          Row line = it.next();
          LOG.info("== " + line);
        }
        LOG.info("==================================");
        LOG.info("== Content of nextLevel - END   ==");
        LOG.info("==================================");
      }

      if (level == 0) {
        /*
           The first level, all nodes as x -> x, which will always join to themselves.
           So the very first time through, we need to filter these out to prevent an infinite loop.
           However, we *do* want to keep them if they have now been identified as a leaf node.
           For example, with the previous example, nextLevel at this point is:

             x [parent:1, child:1, level:0, root:true, leaf:0, category:grocery]
             o [parent:1, child:2, level:1, root:true, leaf:0, category:vegetable]
             o [parent:1, child:2, level:1, root:true, leaf:0, category:vegetable]
             o [parent:1, child:3, level:1, root:true, leaf:0, category:dairy]
             x [parent:2, child:2, level:0, root:false, leaf:0, category:vegetable]
             o [parent:2, child:5, level:1, root:false, leaf:0, category:lettuce]
             o [parent:2, child:6, level:1, root:false, leaf:0, category:tomato]
             o [parent:3, child:3, level:0, root:false, leaf:1, category:dairy]
             x [parent:4, child:4, level:0, root:true, leaf:0, category:fruit]
             o [parent:4, child:6, level:1, root:true, leaf:0, category:tomato]
             o [parent:5, child:5, level:0, root:false, leaf:1, category:lettuce]
             o [parent:6, child:6, level:0, root:false, leaf:1, category:tomato]

           Where the x indicates a row that needs to be filtered out.
         */
        Column whereColumn = null;
        for (Map.Entry<String, String> map : parentChildMapping.entrySet()) {
          if (whereColumn == null) {
            whereColumn = nextLevel.col(map.getKey()).notEqual(nextLevel.col(map.getValue()));
          } else {
            whereColumn = whereColumn
                .or(nextLevel.col(map.getKey()).notEqual(nextLevel.col(map.getValue())));
          }
        }
        whereColumn = whereColumn.or(nextLevel.col(botCol).equalTo(1));
        nextLevel = nextLevel.where(whereColumn);

        if (SHOW_DEBUG_COLUMNS) {
          LOG.info("=========================");
          LOG.info("== whereColumn - Begin  - Level {} ==", level);
          LOG.info("=========================");
          LOG.info("== " + whereColumn);
          LOG.info("=======================");
          LOG.info("== whereColumn - End ==");
          LOG.info("=======================");
        }
        if (SHOW_DEBUG_CONTENT) {
          LOG.info("=====================================================================");
          LOG.info("== Content of nextLevel after where - BEGIN - Level {} ==", level);
          LOG.info("=====================================================================");
          for (Iterator<Row> it = nextLevel.javaRDD().toLocalIterator(); it.hasNext(); ) {
            Row line = it.next();
            LOG.info("== " + line);
          }
          LOG.info("==================================");
          LOG.info("== Content of nextLevel - END   ==");
          LOG.info("==================================");
        }
      }
      flattened = flattened.union(nextLevel);

      if (SHOW_DEBUG_CONTENT) {
        LOG.info("============================================");
        LOG.info("== Content of flattened - BEGIN - Level {} ==", level);
        LOG.info("============================================");
        for (Iterator<Row> it = flattened.javaRDD().toLocalIterator(); it.hasNext(); ) {
          Row line = it.next();
          LOG.info("== " + line);
        }
        LOG.info("=======================================");
        LOG.info("== Content of flattened- Level - END ==");
        LOG.info("=======================================");
      }

      // remove all leaf nodes from next iteration, since they don't have any children.
      currentLevel = nextLevel.where(nextLevel.col(botCol).notEqual(1));
      level++;
    }

    /*
        At this point, the flattened dataset contains duplicates for some leaf rows.
        With the previously mentioned input hierarchy, it will be:

         [1 -> 1, level:0, root:yes, leaf:0]
         [1 -> 2, level:1, root:yes, leaf:0]
         [1 -> 3, level:1, root:yes, leaf:0]
         [1 -> 5, level:2, root:yes, leaf:0]
         [1 -> 5, level:2, root:yes, leaf:1]
         [1 -> 6, level:2, root:yes, leaf:0]
         [1 -> 6, level:2, root:yes, leaf:1]
         [2 -> 2, level:0, root:no, leaf:0]
         [2 -> 5, level:1, root:no, leaf:0]
         [2 -> 5, level:1, root:no, leaf:1]
         [2 -> 6, level:1, root:no, leaf:0]
         [2 -> 6, level:1, root:no, leaf:1]
         [3 -> 3, level:0, root:no, leaf:0]
         [4 -> 4, level:0, root:yes, leaf:0]
         [4 -> 6, level:1, root:yes, leaf:0]
         [4 -> 6, level:1, root:yes, leaf:1]
         [5 -> 5, level:0, root:no, leaf:1]
         [6 -> 6, level:0, root:no, leaf:1]

       Note that for each leaf:1 row, there is a duplicate except it has leaf:0.
       These dupes are removed by grouping on [parent, child] and summing the leaf values.
       This is also where the leaf values are translated to their final strings instead of a 0 or 1.
       If there are multiple paths from one node to another, they will also be de-duplicated down to just a single
       path here, where the level is the minimum level.

         select
           parent,
           child,
           min(level),
           first(root),
           max(leaf) == 0 ? false : true as leaf,
           first(datafield1), first(datafield2), ..., first(datafieldN)
         from flattened
         group by parent, child
     */
    Column[] selectFlattenedColumns = new Column[
        2
            + dataFieldNames.size()
            + 2 * pathFields.size()
            + connectByRootFields.size()];
    int i = 0;
    selectFlattenedColumns[i++] = functions.first(new Column(topCol)).as(topCol);
    selectFlattenedColumns[i++] = functions.when(functions.max(new Column(botCol)).equalTo(0), falseStr)
        .otherwise(trueStr).as(botCol);

    for (String fieldName : dataFieldNames) {
      selectFlattenedColumns[i++] = functions.first(new Column(fieldName)).as(fieldName);
    }

    // Path & Path_length columns
    for (Map<String, String> pathField : pathFields) {
      selectFlattenedColumns[i++] = functions.first(new Column(pathField.get(HierarchyConfig.PATH_FIELD_ALIAS)))
          .as(pathField.get(HierarchyConfig.PATH_FIELD_ALIAS));
      selectFlattenedColumns[i++] = functions.first(new Column(pathField.get(HierarchyConfig.PATH_FIELD_LENGTH_ALIAS)))
          .as(pathField.get(HierarchyConfig.PATH_FIELD_LENGTH_ALIAS));
    }

    // CONNECT_BY_ROOT column
    for (Map<String, String> cbrField : connectByRootFields) {
      selectFlattenedColumns[i++] = functions.first(new Column(cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_ALIAS)))
          .as(cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_ALIAS));
    }

    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("====================================");
      LOG.info("== selectFlattenedColumns - Begin  - Level {} ==", level);
      LOG.info("====================================");
      for (Column col : selectFlattenedColumns) {
        LOG.info("== " + col.toString());
      }
      LOG.info("==================================");
      LOG.info("== selectFlattenedColumns - End ==");
      LOG.info("==================================");
    }

    Column[] groupByColumns = new Column[2 * parentChildMapping.size()];
    i = 0;
    for (Map.Entry<String, String> map : parentChildMapping.entrySet()) {
      groupByColumns[i++] = new Column(map.getKey());
      groupByColumns[i++] = new Column(map.getValue());
    }

    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("============================");
      LOG.info("== groupByColumns - Begin = - Level {} ==", level);
      LOG.info("============================");
      for (Column col : groupByColumns) {
        LOG.info("== " + col.toString());
      }
      LOG.info("==========================");
      LOG.info("== groupByColumns - End ==");
      LOG.info("==========================");
    }

    flattened = flattened.groupBy(groupByColumns)
        .agg(functions.min(new Column(levelCol)).as(levelCol),
            selectFlattenedColumns);

    if (SHOW_DEBUG_CONTENT) {
      LOG.info("==============================================");
      LOG.info("== Content of flattened.groupBy - BEGIN - Level {} ==", level);
      LOG.info("==============================================");
      for (Iterator<Row> it = flattened.javaRDD().toLocalIterator(); it.hasNext(); ) {
        Row line = it.next();
        LOG.info("== " + line);
      }
      LOG.info("========================================");
      LOG.info("== Content of flattened.groupBy - END ==");
      LOG.info("========================================");
    }

    // Perform a final select to make sure fields are in the same order as expected.
    Column[] finalOutputColumns = outputSchema.getFields().stream()
        .map(Schema.Field::getName)
        .map(Column::new)
        .collect(Collectors.toList()).toArray(new Column[outputSchema.getFields().size()]);

    Column notParentWhere = null;
    if (parentRootValues.isEmpty()) {
      for (Map.Entry<String, String> map : parentChildMapping.entrySet()) {
        if (notParentWhere == null) {
          notParentWhere = new Column(map.getKey()).isNotNull();
        } else {
          notParentWhere = notParentWhere.and(new Column(map.getKey()).isNotNull());
        }
      }
    } else {
      for (Map.Entry<String, Object> map : parentRootValues.entrySet()) {
        if (notParentWhere == null) {
          if (map.getValue() == null) {
            notParentWhere = new Column(map.getKey()).isNotNull();
          } else {
            notParentWhere = new Column(map.getKey()).notEqual(map.getValue());
          }
        } else {
          if (map.getValue() == null) {
            notParentWhere = notParentWhere.and(new Column(map.getKey()).isNotNull());
          } else {
            notParentWhere = notParentWhere.and(new Column(map.getKey()).notEqual(map.getValue()));
          }
        }
      }
    }

    if (notParentWhere != null) {
      notParentWhere = notParentWhere.or(new Column(botCol).equalTo(trueStr)).or(new Column(topCol).equalTo(trueStr));
    }

    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("============================");
      LOG.info("== notParentWhere - Begin - Level {} ==", level);
      LOG.info("============================");
      LOG.info("== " + notParentWhere);
      LOG.info("==========================");
      LOG.info("== notParentWhere - End ==");
      LOG.info("==========================");
    }

    // Prepare the columns to sort on, x 2 to account for Parent and Child
    Column[] orderByColumns = new Column[parentChildMapping.size() * 2];
    i = 0;
    for (AbstractMap.SimpleImmutableEntry<String, String> entry : parentChildMappingAsList) {
      orderByColumns[i++] = new Column(entry.getKey()).asc_nulls_first();
    }

    for (AbstractMap.SimpleImmutableEntry<String, String> entry : parentChildMappingAsList) {
      if (siblingOrder.equalsIgnoreCase("ASC")) {
        orderByColumns[i++] = new Column(entry.getValue()).asc_nulls_first();
      } else {
        orderByColumns[i++] = new Column(entry.getValue()).desc_nulls_first();
      }
    }

    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("============================");
      LOG.info("== orderByColumns - Begin - Level {} ==", level);
      LOG.info("============================");
      for (Column column : orderByColumns) {
        LOG.info("== " + column);
      }
      LOG.info("==========================");
      LOG.info("== orderByColumns - End ==");
      LOG.info("==========================");
    }

    if (SHOW_DEBUG_CONTENT) {
      LOG.info("============================================================");
      LOG.info("== Content of flattened before finalOutputColumns - BEGIN ==");
      LOG.info("============================================================");
      for (Iterator<Row> it = flattened.javaRDD().toLocalIterator(); it.hasNext(); ) {
        Row line = it.next();
        LOG.info("== " + line);
      }
      LOG.info("==========================================================");
      LOG.info("== Content of flattened before finalOutputColumns - END ==");
      LOG.info("==========================================================");
    }

    flattened = flattened
        .where(notParentWhere)
        .select(finalOutputColumns)
        .orderBy(orderByColumns);

    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("================================");
      LOG.info("== finalOutputColumns - Begin - Level {} ==", level);
      LOG.info("================================");
      for (Column col : selectFlattenedColumns) {
        LOG.info("== " + col.toString());
      }
      LOG.info("==============================");
      LOG.info("== finalOutputColumns - End ==");
      LOG.info("==============================");
    }

    if (SHOW_DEBUG_CONTENT) {
      LOG.info("===========================================================");
      LOG.info("== Content of flattened after finalOutputColumns - BEGIN ==");
      LOG.info("===========================================================");
      for (Iterator<Row> it = flattened.javaRDD().toLocalIterator(); it.hasNext(); ) {
        Row line = it.next();
        LOG.info("== " + line);
      }
      LOG.info("=========================================================");
      LOG.info("== Content of flattened after finalOutputColumns - END ==");
      LOG.info("=========================================================");
    }

    return flattened.javaRDD().map(row -> DataFrames.fromRow(row, outputSchema));
  }

  private boolean isNotInParentChildMapping(Map<String, String> parentChildMapping, String s) {
    for (Map.Entry<String, String> map : parentChildMapping.entrySet()) {
      if (map.getKey().equals(s) || map.getValue().equals(s)) {
        return false;
      }
    }
    return true;
  }

  /*
     The hierarchy starting points are self referencing paths of level 0:

     With input rows:

         [parent:1, child:2, category:vegetable]
         [parent:1, child:3, category:dairy]
         [parent:2, child:5, category:lettuce]
         [parent:2, child:6, category:tomato]
         [parent:4, child:4, category:fruit]
         [parent:4, child:6, category:tomato]

     The 0 distance paths are:

         [parent:1, child:1, level:0, root:true, leaf:0, category:null]
         [parent:2, child:2, level:0, root:false, leaf:0, category:vegetable]
         [parent:3, child:3, level:0, root:false, leaf:0, category:dairy]
         [parent:4, child:4, level:0, root:true, leaf:0, category:fruit]
         [parent:5, child:5, level:0, root:false, leaf:0, category:lettuce]
         [parent:6, child:6, level:0, root:false, leaf:0, category:tomato]
   */
  private Dataset<Row> getStartingPoints(Dataset<Row> input, List<String> dataFieldNames) {
    // This is all rows where the parent never shows up as a child
    Dataset<Row> levelZero = getNonSelfReferencingRoots(input, dataFieldNames);

    if (SHOW_DEBUG_CONTENT) {
      LOG.info("==================================");
      LOG.info("== Content of levelZero - BEGIN ==");
      LOG.info("==================================");
      for (Iterator<Row> it = levelZero.javaRDD().toLocalIterator(); it.hasNext(); ) {
        Row line = it.next();
        LOG.info("== " + line);
      }
      LOG.info("==================================");
      LOG.info("== Content of levelZero - END   ==");
      LOG.info("==================================");
    }

    /*
       The rest of level 0 is generated with another pass through the data.

         select
           A.child as parent,
           A.child as child,
           0 as level,
           (A.parent == A.child) as root,
           false as leaf,
           A.datafield1,
           ...,
           A.datafieldN

        A distinct is run at the end to remove duplicates, since there can be multiple paths to the same child.
     */
    // 2 * pathFields.size() to account for 2 extra columns for the path & path length
    // 1 * connectByRootFields.size() to account for 1 extra column for the connect by root field
    Column[] columns = new Column[
        2 * parentChildMapping.size()
            + 3
            + dataFieldNames.size()
            + 2 * pathFields.size()
            + connectByRootFields.size()];
    int i = 0;
    Column isRoot = null;
    for (Map.Entry<String, String> map : parentChildMapping.entrySet()) {
      columns[i++] = input.col(map.getValue()).as(map.getKey());
      columns[i++] = input.col(map.getValue()).as(map.getValue());
      if (isRoot == null) {
        isRoot = functions.isnull(input.col(map.getKey()));
      } else {
        isRoot = isRoot.and(functions.isnull(input.col(map.getKey())));
      }
    }

    /*
     == DEPT AS `MGR_DEPT`
     == DEPT AS `DEPT`
     == EMPNO AS `MGR`
     == EMPNO AS `EMPNO`
     == 0 AS `Level`
     == CASE WHEN ((MGR_DEPT IS NULL) AND (MGR IS NULL)) THEN true ELSE false END AS `Root`
     == 0 AS `Leaf`
     == ENAME
     == Lvl
     == ENAME AS `EnamePath`
     == 0 AS `PathLength`
     == ENAME AS `connect_by_root`
     */
    columns[i++] = functions.lit(0).as(levelCol);
    columns[i++] = functions.when(isRoot, trueStr).otherwise(falseStr).as(topCol);
    columns[i++] = functions.lit(0).as(botCol);

    for (String fieldName : dataFieldNames) {
      /*
         If this is a mapped field, the child field should be used as both the parent and child.
         For example, suppose the input data looks like:

           [parent:1, child:1, parentProduct:groceries, childProduct:groceries, supplier:A]
           [parent:1, child:2, parentProduct:groceries, childProduct:produce, supplier:A]

         if the parent child mapping is parentProduct -> childProduct, that means the starting points should be:

           [parent:1, child:1, parentProduct:groceries, childProduct:groceries, supplier:A]
           [parent:2, child:2, parentProduct:produce, childProduct:produce, supplier:A]
       */
      String mappedField = parentChildMapping.get(fieldName);
      if (mappedField != null) {
        columns[i++] = functions.when(isRoot, input.col(fieldName))
            .otherwise(input.col(mappedField)).as(fieldName);
      } else {
        columns[i++] = input.col(fieldName);
      }
    }

    // Path & Path_length columns
    for (Map<String, String> pathField : pathFields) {
      columns[i++] = new Column(pathField.get(HierarchyConfig.VERTEX_FIELD_NAME))
          .as(pathField.get(HierarchyConfig.PATH_FIELD_ALIAS));
      columns[i++] = functions.lit(0).as(pathField.get(HierarchyConfig.PATH_FIELD_LENGTH_ALIAS));
    }

    // CONNECT_BY_ROOT column
    for (Map<String, String> cbrField : connectByRootFields) {
      columns[i++] = new Column(cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_FIELD_NAME))
          .as(cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_ALIAS));
    }

    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("===============================");
      LOG.info("== getStartingPoints - Begin ==");
      LOG.info("===============================");
      for (Column col : columns) {
        if (col != null) {
          LOG.info("== " + col);
        }
      }
      LOG.info("=============================");
      LOG.info("== getStartingPoints - End ==");
      LOG.info("=============================");
    }

    boolean parentIsNull = true;
    for (Object p : parentRootValues.values()) {
      if (p != null) {
        parentIsNull = false;
        break;
      }
    }
    Dataset<Row> distinct;
    if (startWithColumn != null && !parentIsNull) {
      distinct = levelZero;
    } else {
      distinct = levelZero.union(input.select(columns)).distinct();
    }
    if (SHOW_DEBUG_CONTENT) {
      LOG.info("=================================");
      LOG.info("== Content of distinct - BEGIN ==");
      LOG.info("=================================");
      for (Iterator<Row> it = distinct.javaRDD().toLocalIterator(); it.hasNext(); ) {
        Row line = it.next();
        LOG.info("== " + line);
      }
      LOG.info("=================================");
      LOG.info("== Content of distinct - END   ==");
      LOG.info("=================================");
    }
    return distinct;
  }

  /*
    Get roots by looking for rows where the parent never rows up as a child in any other row.
   */
  private Dataset<Row> getNonSelfReferencingRoots(Dataset<Row> input, List<String> dataFieldNames) {
    /*
       These types of roots are rows where the parent never shows up as a child in any other row.
       They are found by running the following query:

       select
         A.parent as parent,
         A.parent as child,
         0 as level,
         true as root,
         false as leaf,
         A.datafield1 as datafield1,
         ...,
         null as datafieldN
       from input as A left outer join (select child from input) as B on A.parent = B.child
       where B.child is null

       Whether a datafield is null or not depends on whether there is a mapping for that field.
       For example, with mapping parentCategory -> category and input rows:

         [parent:1, child:2, parentCategory:plants, category:vegetable, sold:100]
         [parent:2, child:3, parentCategory:vegetable, category:onion, sold:50]

       This results in:

         [parent:1, child:1, level:0, root:true, leaf:false, parentCategory:plants, category:plants, sold:null]

       parentCategory and category both get their value from the parentCategory key in the mapping.
       Every other data field is just null.
    */
    // 2 * pathFields.size() to account for 2 extra columns for the path & path length
    // 1 * connectByRootFields.size() to account for 1 extra column for the connect_by_root
    Column[] columns = new Column[2 * parentChildMapping.size()
        + 3
        + dataFieldNames.size()
        + 2 * pathFields.size()
        + connectByRootFields.size()];
    int i = 0;
    for (Map.Entry<String, String> map : parentChildMapping.entrySet()) {
      columns[i++] = new Column("A." + map.getKey()).as(map.getKey());
      columns[i++] = new Column("A." + map.getValue()).as(map.getValue());
    }

    columns[i++] = functions.lit(0).as(levelCol);
    columns[i++] = functions.lit(trueStr).as(topCol);
    columns[i++] = functions.lit(0).as(botCol);

    Map<String, String> childParentMapping = new HashMap<>();
    for (Map.Entry<String, String> entry : parentChildMapping.entrySet()) {
      childParentMapping.put(entry.getValue(), entry.getKey());
    }
    for (String fieldName : dataFieldNames) {
      /*
         If this is a mapped field, the child field should be used as both the parent and child.
         For example, suppose a root row looks like:

           [parent:1, child:2, parentCategory:plants, category:vegetable, sold:100]

         and there is a mapping of parentCategory -> category
         that means the result should be:

           [parent:1, child:1, level:0, root:true, leaf:false, parentCategory:plants, category:plants, sold:null]
       */
      String mappedField = childParentMapping.get(fieldName);
      if (mappedField != null) {
        // in example above, this is for setting parentCategory(plants) as category
        columns[i++] = new Column("A." + mappedField).as(fieldName);
      } else if (parentChildMapping.containsKey(fieldName)) {
        // in example above, this is for setting parentCategory as itself
        columns[i++] = new Column("A." + fieldName).as(fieldName);
      } else {
        // in example above, this is for setting null as sold
        columns[i++] = functions.lit(null).as(fieldName);
      }
    }

    // Path & Path_length columns
    for (Map<String, String> pathField : pathFields) {
      columns[i++] = new Column("A." + pathField.get(HierarchyConfig.VERTEX_FIELD_NAME))
          .as(pathField.get(HierarchyConfig.PATH_FIELD_ALIAS));
      columns[i++] = functions.lit(0).as(pathField.get(HierarchyConfig.PATH_FIELD_LENGTH_ALIAS));
    }

    // CONNECT_BY_ROOT column
    for (Map<String, String> cbrField : connectByRootFields) {
      columns[i++] = new Column("A." + cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_FIELD_NAME))
          .as(cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_ALIAS));
    }

    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("========================================");
      LOG.info("== getNonSelfReferencingRoots - Begin ==");
      LOG.info("========================================");
      for (Column col : columns) {
        if (col != null) {
          LOG.info("==  " + col);
        }
      }
      LOG.info("======================================");
      LOG.info("== getNonSelfReferencingRoots - End ==");
      LOG.info("======================================");
    }

    /*
       we only need childCol from the input and not any of the other fields
       drop all the other fields before the join so that they don't need to be shuffled
       across the cluster, only to be dropped after the join completes.
     */
    Column[] selectColumns = new Column[parentChildMapping.size()];
    i = 0;
    for (Map.Entry<String, String> map : parentChildMapping.entrySet()) {
      selectColumns[i++] = new Column(map.getValue());
    }
    Dataset<Row> children = input.select(selectColumns);

    if (SHOW_DEBUG_CONTENT) {
      LOG.info("==============================");
      LOG.info("== Content of input - BEGIN ==");
      LOG.info("==============================");
      for (Iterator<Row> it = input.javaRDD().toLocalIterator(); it.hasNext(); ) {
        Row line = it.next();
        LOG.info("== " + line);
      }
      LOG.info("============================");
      LOG.info("== Content of input - END ==");
      LOG.info("============================");
    }
    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("===========================");
      LOG.info("== selectColumns - BEGIN ==");
      LOG.info("===========================");
      for (Column column : selectColumns) {
        LOG.info("==  " + column.toString());
      }
      LOG.info("=========================");
      LOG.info("== selectColumns - END ==");
      LOG.info("=========================");
    }

    if (SHOW_DEBUG_CONTENT) {
      LOG.info("=================================");
      LOG.info("== Content of children - BEGIN ==");
      LOG.info("=================================");
      for (Iterator<Row> it = children.javaRDD().toLocalIterator(); it.hasNext(); ) {
        Row line = it.next();
        LOG.info("== " + line);
      }
      LOG.info("===============================");
      LOG.info("== Content of children - END ==");
      LOG.info("===============================");
    }

    Dataset<Row> joined;
    Column broadCastJoinColumns = null;
    Column broadCastWhereColumns = null;
    for (Map.Entry<String, String> map : parentChildMapping.entrySet()) {
      if (broadCastJoinColumns == null) {
        broadCastJoinColumns = new Column("A." + map.getKey()).equalTo(new Column("B." + map.getValue()));
        broadCastWhereColumns = new Column("B." + map.getValue()).isNull();
      } else {
        broadCastJoinColumns = broadCastJoinColumns
            .and(new Column("A." + map.getKey()).equalTo(new Column("B." + map.getValue())));
        broadCastWhereColumns = broadCastWhereColumns.and(new Column("B." + map.getValue()).isNull());
      }
    }

    startWithColumn = getStartWithColumn(inputSchema, "A");

    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("==================================");
      LOG.info("== broadCastJoinColumns - BEGIN ==");
      LOG.info("==================================");
      if (broadCastJoinColumns != null) {
        LOG.info("==  " + broadCastJoinColumns);
      }
      LOG.info("================================");
      LOG.info("== broadCastJoinColumns - END ==");
      LOG.info("================================");

      LOG.info("===================================");
      LOG.info("== broadCastWhereColumns - BEGIN ==");
      LOG.info("===================================");
      if (broadCastWhereColumns != null) {
        LOG.info("==  " + broadCastWhereColumns);
      }
      LOG.info("=================================");
      LOG.info("== broadCastWhereColumns - END ==");
      LOG.info("=================================");

      LOG.info("=============================");
      LOG.info("== startWithColumn - BEGIN ==");
      LOG.info("=============================");
      if (startWithColumn != null) {
        LOG.info("==  " + startWithColumn);
      }
      LOG.info("===========================");
      LOG.info("== startWithColumn - END ==");
      LOG.info("===========================");
    }

    if (broadcastJoin) {
      if (startWithColumn == null) {
        joined = input.alias("A").join(
            broadcast(children.alias("B")),
            broadCastJoinColumns, "leftouter")
            .where(broadCastWhereColumns)
            .select(columns);
      } else {
        joined = input.alias("A").join(
            children.alias("B"),
            broadCastJoinColumns, "inner")
            .where(startWithColumn)
            .select(columns);
      }
    } else {
      if (startWithColumn == null) {
        joined = input.alias("A").join(
            children.alias("B"),
            broadCastJoinColumns, "leftouter")
            .where(broadCastWhereColumns)
            .select(columns);
      } else {
        joined = input.alias("A").join(
            children.alias("B"),
            broadCastJoinColumns, "inner")
            .where(startWithColumn)
            .select(columns);
      }
    }

    if (SHOW_DEBUG_CONTENT) {
      LOG.info("============================");
      LOG.info("== joined content - BEGIN ==");
      LOG.info("============================");
    }

    // Save the value for the root node, we'll use them at the very end to remove duplicates but keep the root node
    for (Iterator<Row> it = joined.javaRDD().toLocalIterator(); it.hasNext(); ) {
      Row row = it.next();
      for (Map.Entry<String, String> map : parentChildMapping.entrySet()) {
        parentRootValues.put(map.getKey(), row.getAs(map.getKey()));
        childRootValues.put(map.getValue(), row.getAs(map.getValue()));
      }
      if (SHOW_DEBUG_CONTENT) {
        LOG.info("== " + row);
      }
    }
    if (SHOW_DEBUG_CONTENT) {
      LOG.info("==========================");
      LOG.info("== joined content - END ==");
      LOG.info("==========================");

      LOG.info("==============================");
      LOG.info("== parentRootValues - BEGIN ==");
      LOG.info("==============================");
      for (Map.Entry<String, Object> map : parentRootValues.entrySet()) {
        LOG.info("== parentRootValues: " + map.getKey() + " -> " + map.getValue());
      }
      LOG.info("============================");
      LOG.info("== parentRootValues - END ==");
      LOG.info("============================");

      LOG.info("=============================");
      LOG.info("== childRootValues - BEGIN ==");
      LOG.info("=============================");
      for (Map.Entry<String, Object> map : childRootValues.entrySet()) {
        LOG.info("== childRootValues: " + map.getKey() + " -> " + map.getValue());
      }
      LOG.info("===========================");
      LOG.info("== childRootValues - END ==");
      LOG.info("===========================");
    }

    return joined;
  }

  private boolean isEmpty(Dataset<Row> dataset) {
    // unfortunately there is no way to check if a dataset is empty without running a spark job
    // this, however, is much more performant than performing a count(), which would require a pass over all the data.
    return dataset.takeAsList(1).isEmpty();
  }

  /**
   * Parse the start with condition to build a Column object that can be passed to the initialization stage
   *
   * @return Column object
   */
  public Column getStartWithColumn(Schema inputSchema, String setName) {
    Column column = null;
    List<String> conditions = startWithConditions;

    // If there's nothing to process, return right away.
    if (conditions.isEmpty() || inputSchema == null || inputSchema.getFields() == null || setName == null) {
      return null;
    }

    // If it's not empty, add the dot
    if (!setName.isEmpty()) {
      setName += ".";
    }

    // Create a Set to look up the column names in the inpu schema
    Set<String> columnNames = inputSchema.getFields()
        .stream().map(f -> f.getName().toUpperCase()).collect(Collectors.toSet());
    for (String condition : conditions) {
      if (!Strings.isNullOrEmpty(condition)) {
        String[] splits = condition.replace("=", " = ").trim().split("\\s++");
        if (splits.length == 3) {
          // The only recognized conditions are:
          // <column_name> is null
          // <column_name> = <value>
          String columnName = splits[0].toUpperCase();
          String operator = splits[1].toUpperCase();
          String value = splits[2];

          if (!columnNames.contains(columnName)) {
            LOG.warn("'Start with' Condition " + condition + " was not used because the column " + columnName
                + " was not found in the input schema");
            continue;
          }

          if (operator.equals("=")) {
            if (column == null) {
              column = new Column(setName + columnName.toUpperCase()).equalTo(value);
            } else {
              column = column.and(new Column(setName + columnName.toUpperCase()).equalTo(value));
            }
          } else if ((operator.equals("IS") && value.equalsIgnoreCase("null"))) {
            if (column == null) {
              column = new Column(setName + columnName.toUpperCase()).isNull();
            } else {
              column = column.and(new Column(setName + columnName.toUpperCase()).isNull());
            }
          } else {
            // Error, don't know what it is
            LOG.warn("'Start with' Condition " + condition + " was not used because the operator " + operator
                + " was not recognized.");
          }
        } else {
          // Error, don't know what it is
          LOG.warn("'Start with' Condition " + condition + " was not used because the result of splitting the string " +
              "yielded more than 3 entries.");
        }
      }
    }
//    column = column.and(new Column("A.TEST").equalTo("ABCD"));
    return column;
  }

}
