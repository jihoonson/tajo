create external table schemaless using json with ('compression.codec'='none') partition by column (id int8) location 'file:///schemaless'