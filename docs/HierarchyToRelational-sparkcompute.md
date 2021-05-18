# Hierarchical to Relational Plugin

Description
----------- 
Provides the ability to flatten a hierarchical data model into a relational model.

Use Case
--------
This plugin can be used when a user needs to flatten a hierarchical data model to a relational model.
It expects incoming records to represent a direct parent-child relationship from one element to another.
The input data can contain multiple root elements, but must not contain any cycles.
The plugin will flatten the hierarchy such that there is an output record from each element to itself and
to all of its descendants. Each output record will include the distance from the element to its descendant.

Properties
----------
**Parent field:** Specifies the field from the input schema that should be used as the parent in the hierarchical model.
Should always contain a single, non-null root element in the hierarchy

**Child field:** Specifies the field from the input schema that should be used as the child in the hierarchical model.

**Parent -> Child fields mapping:** Specifies a parent-child relationship between fields in the input. This is used
to indicate that some input fields are attributes of the parent while some are attributes of the child. This
is used during the flattening process to set the right value for the data fields.

When using a Macro, pass the parent -> child mapping as follows:
- If using a single relationship: parent_field_name=child_field_name
- If using multiple relationships: parent_field_name1=child_field_name1;parent_field_name2=child_field_name2

**Start with:** Define boolean expressions that define the root node to start from. This will limit the result set to 
nodes that are connected to that root node only. Valid conditions are:

- Input_field_name = Value
- Input_field_name is null

When providing more than one condition, a logical 'AND' will be applied. When using the first construct, do not use
quotes, even for character or string values.
The Input_field_name is case-insensitive, 'is null' is also case-insensitive, but the 'Value' is used as-is and is thus, 
case-sensitive.  

When using a Macro, pass the start with conditions as follows:
- If using a single relationship: Input_field_name=Value
- If using multiple relationships: Input_field_name1=Value1;Input_field_name2=Value2

**Level Field Name:** The name of the field that should contain the level in the hierarchy starting at a particular
element in the tree. The level is the distance from the parent to the child. If there are multiple paths from
a parent to a child, only a single record is output, where the level is set to the minimum level of all paths. 

**Top Field Name:** The name of the field that determines whether a node is the root element or the top-most element in
the hierarchy. The input data should always contain at least one non-null root node. For that node, this field is true,
while it is marked false for all other nodes in the hierarchy. This will only be true when both the parent and child
are a root element. It will be false when the parent is a root, but the child is not.

**Bottom Field Name:** The name of the field that determines whether a node is a leaf element or the bottom-most element
in the hierarchy. The input data can contain multiple leaf nodes. This will be true whenever the child is a leaf element,
even if the parent is not a leaf.

**True value:** The value that denotes truth in the Top and Bottom fields.

**False value:** The value that denotes false in the Top and Bottom fields.

**Max depth:** The maximum depth up to which the data should be flattened. If a node is reached at a deeper level, 
an error will be thrown.

**Connect by root:** Name of the field that will be used to retrieve the value for the root node of the path from the 
root to a node.

**Sibling order:** Toggle that indicates how sibling in a hierarchy need to be sorted in the output.

**Path fields:** Parameters to use to build a string that shows the path from the root node to the current node. 
Parameters are:

- Field name for vertices: Field name whose value will be used along the way to build the entire path from the root of 
  the branch to the current node.
- Path separator: Usually a single character that separates the different values picked up along the path.
- Alias for the path field: Alias to use to name the column containing the resulting path field.
- Alias for the path length field: Alias to use to name the column containing the length of the path.

When using a Macro, pass the start with conditions as follows:
vertexFieldName=<input_field_name>;pathSeparator=<character>;pathFieldAlias=<alias_name>;pathFieldLengthAlias=<alias_name>

**Broadcast join:** Performs a Spark in-memory broadcast join.

Example
-------
Consider the dataset below depicting sales (numbers) of specific products in every quarter for a grocery store.

|ParentId|ChildId|ParentProduct|ChildProduct|Supplier|Sales|
|--------|-------|-------------|------------|--------|-----|
|1|2|Groceries|Produce|A|50|
|1|3|Groceries|Dairy|B|40|
|2|4|Produce|Vegetables|C|50|
|4|6|Vegetables|Onion|E|30|

With the following configuration:

**Parent field**: `ParentId`

**Child field**: `ChildId`

**Parent -> Child fields mapping**: `ParentProduct=ChildProduct`

**Level Field Name**: `Level`

**Top Field Name**: `Root`

**Bottom Field Name**: `Leaf`

**True value**: `Yes`

**False value**: `No`

the following output is generated:

|ParentId|ChildId|ParentProduct|ChildProduct|Supplier|Sales|Level|Root|Leaf|
|--------|-------|-------------|------------|--------|-----|-----|--------|-----------|
|1|1|Groceries|Groceries|null|null|0|Yes|No|
|1|2|Groceries|Produce|A|50|1|No|No|
|1|3|Groceries|Dairy|B|40|1|No|Yes|
|1|4|Groceries|Vegetables|C|50|2|No|No|
|1|6|Groceries|Onion|E|30|3|No|Yes|
|2|2|Produce|Produce|A|50|0|No|No|
|2|4|Produce|Vegetables|C|50|1|No|No|
|2|6|Produce|Onion|E|30|2|No|Yes|
|3|3|Dairy|Dairy|B|40|0|No|Yes|
|4|4|Vegetables|Vegetables|C|50|0|No|No|
|4|4|Vegetables|Onion|E|30|1|No|Yes|
|6|6|Onion|Onion|E|30|0|No|Yes|

Note that the ParentProduct and ChildProduct are set appropriately for the self referencing rows.
For example, when the ParentID and ChildId is 1, the ParentProduct and ChildProduct fields are set to
'Groceries'. This is because there is a parent -> child mapping from ParentProduct to ChildProduct.
Without this mapping, both the ParentProduct and ChildProduct fields would be set to null for that row.

The dataset below is similar to the previous example. The only difference is there is a record in the
input where the ParentId and ChildId are both 1.

|ParentId|ChildId|ParentProduct|ChildProduct|Supplier|Sales|
|--------|-------|-------------|------------|--------|-----|
|1|1|Groceries|Groceries|A|0|
|1|2|Groceries|Produce|A|50|
|1|3|Groceries|Dairy|B|40|
|2|4|Produce|Vegetables|C|50|
|4|6|Vegetables|Onion|E|30|

With the same configuration settings, the plugin will generate almost the same output.
The difference is on the root record, where the Supplier and Sales fields are taken from the
input data instead of being null as in the previous example.

|ParentId|ChildId|ParentProduct|ChildProduct|Supplier|Sales|Level|Root|Leaf|
|--------|-------|-------------|------------|--------|-----|-----|--------|-----------|
|1|1|Groceries|Groceries|A|0|0|Yes|No|
|1|2|Groceries|Produce|A|50|1|No|No|
|1|3|Groceries|Dairy|B|40|1|No|Yes|
|1|4|Groceries|Vegetables|C|50|2|No|No|
|1|6|Groceries|Onion|E|30|3|No|Yes|
|2|2|Produce|Produce|A|50|0|No|No|
|2|4|Produce|Vegetables|C|50|1|No|No|
|2|6|Produce|Onion|E|30|2|No|Yes|
|3|3|Dairy|Dairy|B|40|0|No|Yes|
|4|4|Vegetables|Vegetables|C|50|0|No|No|
|4|4|Vegetables|Onion|E|30|1|No|Yes|
|6|6|Onion|Onion|E|30|0|No|Yes|
