DECLARE
  TYPE ruc IS TABLE OF VARCHAR2(30) INDEX BY VARCHAR2(30);
  numero_ruc ruc;
  comprador ruc;
  p_CODIGO_VENDEDOR VARCHAR2(30);
  p_CODIGO_COMPRADOR VARCHAR2(30);
  p_CODIGO_COMPROBANTE FACTURA.CODIGO_COMPROBANTE%type;
  p_FECHA_EMISION FACTURA.FECHA_EMISION%type DEFAULT NULL;
  p_BASE_GRAVADA FACTURA.BASE_GRAVADA%type DEFAULT NULL;
  p_IDENTIFICACION_COMPRADOR FACTURA.IDENTIFICACION_COMPRADOR%type DEFAULT NULL;
  p_TOTAL_SIN_IMPUESTOS FACTURA.TOTAL_SIN_IMPUESTOS%type DEFAULT NULL;
  p_MONTO_IVA FACTURA.MONTO_IVA%type DEFAULT NULL;
  p_BASE_TARIFA_0 FACTURA.BASE_TARIFA_0%type DEFAULT NULL;
  p_NUMERO_RUC_EMISOR FACTURA.NUMERO_RUC_EMISOR%type DEFAULT NULL;
  p_IMPORTE_TOTAL FACTURA.IMPORTE_TOTAL%type DEFAULT NULL;
  p_hora_inicial date;
  p_hora_final date;
  p_tiempo_transcurrido int;
  p_segundos_espera int;
  p_segundos_entre_cada_factura float;
  
  BEGIN
  
    /* Se registra los segundos que van a transcurrir hasta que se termine de ejecutar el ingreso de facturas */
    p_segundos_espera := 10;
    
    /* Se registra los segundos que van a transcurrir entre cada ingreso de factura */
    p_segundos_entre_cada_factura := 0.05;
    
    /* Se registran códigos de 30 vendedores */
    numero_ruc(1) := '1220082247036';
    numero_ruc(2) := '1727935415836';
    numero_ruc(3) := '1967324274043';
    numero_ruc(4) := '1818048779732';
    numero_ruc(5) := '1016632378290';
    numero_ruc(6) := '2167261616426';
    numero_ruc(7) := '1742462460194';
    numero_ruc(8) := '1913940448265';
    numero_ruc(9) := '1372272925406';
    numero_ruc(10) := '1337178905827';
    numero_ruc(11) := '1232020150822';
    numero_ruc(12) := '1295908639479';
    numero_ruc(13) := '1570528582385';
    numero_ruc(14) := '1692535109929';
    numero_ruc(15) := '1476690215863';
    numero_ruc(16) := '1459649022486';
    numero_ruc(17) := '1000857272905';
    numero_ruc(18) := '1326571672012';
    numero_ruc(19) := '1650475811640';
    numero_ruc(20) := '1245605954709';
    numero_ruc(21) := '2085766009224';
    numero_ruc(22) := '1319796817544';
    numero_ruc(23) := '1685625662003';
    numero_ruc(24) := '1004927423826';
    numero_ruc(25) := '1997498818867';
    numero_ruc(26) := '2075925469694';
    numero_ruc(27) := '1187240555401';
    numero_ruc(28) := '1012161513045';
    numero_ruc(29) := '1952691464185';
    numero_ruc(30) := '2184242153835';
    
    /* Se registran códigos de 30 compradores */
    comprador(1) := '179206330';
    comprador(2) := '174041756';
    comprador(3) := '172787719';
    comprador(4) := '178686938';
    comprador(5) := '176868208';
    comprador(6) := '174087510';
    comprador(7) := '172620409';
    comprador(8) := '176081457';
    comprador(9) := '172006585';
    comprador(10) := '172298287';
    comprador(11) := '170824856';
    comprador(12) := '178508074';
    comprador(13) := '173680028';
    comprador(14) := '177249687';
    comprador(15) := '172660288';
    comprador(16) := '176170294';
    comprador(17) := '176592965';
    comprador(18) := '170161967';
    comprador(19) := '171492170';
    comprador(20) := '174679653';
    comprador(21) := '178287275';
    comprador(22) := '172631938';
    comprador(23) := '178720472';
    comprador(24) := '171393873';
    comprador(25) := '176582242';
    comprador(26) := '179555054';
    comprador(27) := '179518712';
    comprador(28) := '174678913';
    comprador(29) := '171041546';
    comprador(30) := '170412085';
    
    /* se registra la hora inicial antes de comenzar la generación de facturas */
    select sysdate into p_hora_inicial
    from dual;
    
    /* se inicia un loop dentro del cual se registrará 1 factura por cada loop */
    loop
    
      /* se selecciona el máximo código de factura (comprobante) */
      select nvl(max(CODIGO_COMPROBANTE),0) + 1 
      into p_CODIGO_COMPROBANTE
      from FACTURA;
      
      /* se selecciona de manera aleatoria un código de vendedor */
      select round(dbms_random.value(1, 30),0)
      into p_CODIGO_VENDEDOR
      from dual;
  
      /* se selecciona de manera aleatoria un código de comprador */
      select round(dbms_random.value(1, 30),0)
      into p_CODIGO_COMPRADOR
      from dual;
  
      /* se genera de manera aleatoria un valor de base gravada que esté entre 0 y 500 sin decimales*/
      select round(dbms_random.value(0, 500), 0) 
      into p_BASE_GRAVADA
      from dual;
    
      /* se genera de manera aleatoria un valor de base que no grava impuesto que esté entre 0 y 100 sin decimales*/
      select round(dbms_random.value(0, 100), 0) 
      into p_BASE_TARIFA_0
      from dual;
      
      /* se calcula el monto del impuesto al valor agregador a partir de la base gravada */
      select p_BASE_GRAVADA * 0.12
      into p_MONTO_IVA
      from dual;
      
      /* se calcula el total sin impuestos, que es la suma de la base gravada mas la base que no grava impuestos */
      select p_BASE_GRAVADA + p_BASE_TARIFA_0
      into p_TOTAL_SIN_IMPUESTOS
      from dual;
      
      /* se calcula el importe total de la factura, que es el total sin impuestos mas el monto de impuesto al valor agregado */
      select p_TOTAL_SIN_IMPUESTOS + p_MONTO_IVA
      into p_IMPORTE_TOTAL
      from dual;
      
      /* se inserta el registro de factura en la base de datos */
      insert
      into FACTURA
        (
          CODIGO_COMPROBANTE ,
          FECHA_EMISION ,
          BASE_GRAVADA ,
          IDENTIFICACION_COMPRADOR ,
          TOTAL_SIN_IMPUESTOS ,
          MONTO_IVA ,
          BASE_TARIFA_0 ,
          NUMERO_RUC_EMISOR ,
          IMPORTE_TOTAL
        )
        values
        (
          p_CODIGO_COMPROBANTE ,
          sysdate ,
          p_BASE_GRAVADA ,
          comprador(p_CODIGO_COMPRADOR) ,
          p_TOTAL_SIN_IMPUESTOS ,
          p_MONTO_IVA ,
          p_BASE_TARIFA_0 ,
          numero_ruc(p_CODIGO_VENDEDOR) ,
          p_IMPORTE_TOTAL
        );
        
        /* se asegura la transacción en la base de datos */
        commit;
        
        /* se registra la hora final en la cual se generó el registro en la base de datos */
        select sysdate into p_hora_final
        from dual;
        
        /* se calcula el tiempo transcurrido en segundos desde que se generó el primer registro de facturación*/
        select (p_hora_final - p_hora_inicial)*24*60*60 into p_tiempo_transcurrido
        from dual;
        
        /* se espera los segundos configurados para el siguiente ingreso de factura */
        DBMS_LOCK.Sleep( p_segundos_entre_cada_factura );
        
        /* se sale del lazo cuando han transcurrido los segundos configurados */
        exit when p_tiempo_transcurrido >= p_segundos_espera;
        end loop;
      
        
        Dbms_Output.Put_Line(p_tiempo_transcurrido);
        --+000000000 00:00:25.000000000 25 segundos para registrar 50 mil facturas
END;