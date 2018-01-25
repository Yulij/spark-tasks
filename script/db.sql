CREATE TABLE CATEGORY
(
  ID   INT PRIMARY KEY AUTO_INCREMENT,
  NAME VARCHAR(30)
);

CREATE TABLE PRODUCT
(
  ID          INT AUTO_INCREMENT PRIMARY KEY,
  BRAND       VARCHAR(20) DEFAULT 'UNKNOWN' NOT NULL,
  CATEGORY_ID INT                           NOT NULL,
  COST        DOUBLE DEFAULT '0'            NOT NULL,
  CONSTRAINT PRODUCT_CATEGORY_fk
  FOREIGN KEY (CATEGORY_ID) REFERENCES CATEGORY (ID)
);
CREATE INDEX PRODUCT_CATEGORY_fk
  ON PRODUCT (CATEGORY_ID);

CREATE TABLE `ORDER`
(
  ID              INT(11) PRIMARY KEY AUTO_INCREMENT,
  CUSTOMER_ID     INT(11),
  CREATION_TMST   TIMESTAMP,
  COMPLETION_TMST TIMESTAMP
);

CREATE TABLE ORDER_ITEM
(
  ID           INT AUTO_INCREMENT PRIMARY KEY,
  PRODUCT_ID   INT    NULL,
  QUANTITY     INT(3) NULL,
  PROMOTION_ID INT    NULL,
  COST         DOUBLE NULL,
  ORDER_ID     INT    NOT NULL,
  CONSTRAINT ORDER_fk
  FOREIGN KEY (ORDER_ID) REFERENCES `ORDER` (ID)
);
CREATE INDEX ORDER_fk ON ORDER_ITEM (ORDER_ID);

CREATE OR REPLACE VIEW ORDER_ITEM_VIEW AS
  SELECT
    p.CATEGORY_ID,
    c.NAME      AS CATEGORY,
    p.BRAND     AS BRAND,
    oi.QUANTITY AS QUANTITY,
    oi.COST
  FROM ORDER_ITEM oi
    LEFT JOIN `ORDER` o ON oi.ORDER_ID = o.ID
    LEFT JOIN PRODUCT p ON oi.PRODUCT_ID = p.ID
    LEFT JOIN CATEGORY c ON p.CATEGORY_ID = c.ID;


SELECT * FROM (
                SELECT
                  a.CATEGORY_ID,
                  a.CATEGORY,
                  a.BRAND,
                  a.TOTAL,
                  (SELECT 1 + count(*)
                   FROM (SELECT
                           oi.CATEGORY_ID,
                           oi.CATEGORY,
                           oi.BRAND,
                           sum(oi.COST) AS TOTAL
                         FROM ORDER_ITEM_VIEW oi
                         GROUP BY CATEGORY, BRAND) AS b
                   WHERE b.CATEGORY_ID = a.CATEGORY_ID AND b.TOTAL > a.TOTAL) AS RANK
                FROM (SELECT
                        oiv.CATEGORY_ID,
                        oiv.CATEGORY,
                        oiv.BRAND,
                        sum(oiv.COST) AS TOTAL
                      FROM ORDER_ITEM_VIEW oiv
                      GROUP BY CATEGORY, BRAND) AS a) AS x
WHERE x.RANK <= 3
ORDER BY x.TOTAL DESC;