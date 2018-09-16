/* Se elimina la clave primaria de manera recursiva */
ALTER TABLE FACTURA DROP PRIMARY KEY CASCADE;

/* Se elimina la tabla de facturas y todos sus constraint asociados */
DROP TABLE FACTURA CASCADE CONSTRAINTS;

/* Se crea la tabla que contiene la información de las facturas */
CREATE TABLE FACTURA
(
  CODIGO_COMPROBANTE              NUMBER(15,0),
  FECHA_EMISION                   TIMESTAMP,
  IDENTIFICACION_COMPRADOR        VARCHAR2(20),
  TOTAL_SIN_IMPUESTOS             NUMBER(15,2),
  IMPORTE_TOTAL                   NUMBER(15,2),
  BASE_GRAVADA                    NUMBER(15,2),
  BASE_TARIFA_0                   NUMBER(15,2),
  MONTO_IVA                       NUMBER(15,2),
  NUMERO_RUC_EMISOR               VARCHAR2(13)
);

/* Se crea un índice único sobre el codigo de comprobante */
CREATE UNIQUE INDEX INDICE_1 ON FACTURA (CODIGO_COMPROBANTE);

/* Se crea un índice sobre la fecha de emisión de la factura */
CREATE INDEX INDICE_2 ON FACTURA(FECHA_EMISION);

/* Se añade la clave primaria a la tabla de factura */
ALTER TABLE FACTURA ADD ( PRIMARY KEY (CODIGO_COMPROBANTE));
  
  