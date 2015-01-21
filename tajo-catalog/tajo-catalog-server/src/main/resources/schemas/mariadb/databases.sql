CREATE TABLE DATABASES_ (
  DB_ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  DB_NAME VARCHAR(128) BINARY NOT NULL UNIQUE,
  SPACE_ID INT NOT NULL,
  FOREIGN KEY (SPACE_ID) REFERENCES TABLESPACES (SPACE_ID),
  UNIQUE INDEX IDX_NAME (DB_NAME)
)
