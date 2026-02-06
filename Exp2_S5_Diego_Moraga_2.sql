/* Desarrollo de la actividad correspondiente a la semana 5 del curso de Programacion de 
Bases de Datos y que corresponde a la segunda actividad sumativa del curso.

En esta ocasión se nos pide ayudar a la empresa "All The Best" que es una empresa de retail 
chilena.

Se entregan las reglas de negocio y las instrucciones para la creación del bloque PL/SQL.

Se hace presente que conforme a las instrucciones recibidas se irá comentando cada uso de
los conceptos vistos en el curso y que fueron implementado en el bloque PL/SQL.
*/

-- A continuación se crea bloque PL/SQL conforme a las instrucciones de la actividad

SET SERVEROUTPUT ON;

/* Conforme a las instrucciones se ingresa el período de ejecución de forma paramétrica
mediante el uso de una variable BIND 

*/
VARIABLE b_periodo NUMBER;
EXEC :b_periodo := EXTRACT(YEAR FROM SYSDATE);

DECLARE
    /* Conforme a las instrucciones del caso el valor de los tipos de transacciones de 
    tarjeta se ingresa utilizando un  VARRAY */

    TYPE t_tipos_transac IS VARRAY(2) OF VARCHAR2(50);
    v_tipos t_tipos_transac := t_tipos_transac('Avance en Efectivo', 'Súper Avance en Efectivo');

    -- Registro PL/SQL para el resumen
    TYPE r_resumen IS RECORD (
        mes_anno      VARCHAR2(6),
        tipo_transac  VARCHAR2(40),
        monto_total   NUMBER(10),
        aporte_total  NUMBER(10)
    );
    v_reg_res r_resumen;

    -- Se definen los cursores explícitos 

    CURSOR c_detalle IS
        SELECT cl.numrun, cl.dvrun, t.nro_tarjeta, t.nro_transaccion, 
               t.fecha_transaccion, tp.nombre_tptran_tarjeta, t.monto_transaccion
        FROM CLIENTE cl
        JOIN TARJETA_CLIENTE tc ON cl.numrun = tc.numrun
        JOIN TRANSACCION_TARJETA_CLIENTE t ON tc.nro_tarjeta = t.nro_tarjeta
        JOIN TIPO_TRANSACCION_TARJETA tp ON t.cod_tptran_tarjeta = tp.cod_tptran_tarjeta
        WHERE EXTRACT(YEAR FROM t.fecha_transaccion) = :b_periodo
          AND (tp.nombre_tptran_tarjeta = v_tipos(1) OR tp.nombre_tptran_tarjeta = v_tipos(2))
        ORDER BY t.fecha_transaccion ASC, cl.numrun ASC;

    CURSOR c_resumen_agrupado(p_anno NUMBER) IS
        SELECT TO_CHAR(t.fecha_transaccion, 'MMYYYY') as mes_anno,
               tp.nombre_tptran_tarjeta,
               SUM(t.monto_transaccion) as suma_monto
        FROM TRANSACCION_TARJETA_CLIENTE t
        JOIN TIPO_TRANSACCION_TARJETA tp ON t.cod_tptran_tarjeta = tp.cod_tptran_tarjeta
        WHERE EXTRACT(YEAR FROM t.fecha_transaccion) = p_anno
          AND tp.nombre_tptran_tarjeta IN (v_tipos(1), v_tipos(2))
        GROUP BY TO_CHAR(t.fecha_transaccion, 'MMYYYY'), tp.nombre_tptran_tarjeta
        ORDER BY 1 ASC, 2 ASC;

    v_porc_aporte      NUMBER(3);
    v_aporte_indiv     NUMBER(10);
    v_total_esperado   NUMBER := 0;
    v_iteraciones      NUMBER := 0;

    -- Se incorpora el manejo de excepciones solicitado en el caso. 
    e_error_pk EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_error_pk, -00001);

BEGIN
    -- Se truncan las tablas conforme a las instrucciones del caso.
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_APORTE_SBIF';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_APORTE_SBIF';

    -- Se realiza un conteo para validación de las transacciones realizadas.
    SELECT COUNT(*) INTO v_total_esperado
    FROM TRANSACCION_TARJETA_CLIENTE t
    JOIN TIPO_TRANSACCION_TARJETA tp ON t.cod_tptran_tarjeta = tp.cod_tptran_tarjeta
    WHERE EXTRACT(YEAR FROM t.fecha_transaccion) = :b_periodo
      AND tp.nombre_tptran_tarjeta IN (v_tipos(1), v_tipos(2));

    -- Detalle del procesamiento
    FOR r_det IN c_detalle LOOP  -- Se itera sobre cada registro del cursor c_detalle
        -- Se realiza el cálculo del aporte a la SBIF conforme a las reglas de negocio.
        BEGIN
            SELECT porc_aporte_sbif INTO v_porc_aporte
            FROM TRAMO_APORTE_SBIF
            WHERE r_det.monto_transaccion BETWEEN tramo_inf_av_sav AND tramo_sup_av_sav;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN v_porc_aporte := 0;
        END;

        v_aporte_indiv := ROUND(r_det.monto_transaccion * (v_porc_aporte / 100));

        /* En base a las instrucciones recibidas los resultados del proceso son almacenados
        en las tablas DETALLE_APORTE_SBIF y RESUMEN_APORTE_SBIF respectivamente, respetando 
        el formato dado en las imágenes del caso.
        
         */
        INSERT INTO DETALLE_APORTE_SBIF VALUES (
            r_det.numrun, r_det.dvrun, r_det.nro_tarjeta, r_det.nro_transaccion,
            r_det.fecha_transaccion, r_det.nombre_tptran_tarjeta, r_det.monto_transaccion,
            v_aporte_indiv
        );
        
        v_iteraciones := v_iteraciones + 1;
    END LOOP;

    -- PROCESAMIENTO RESUMEN
    FOR r_res IN c_resumen_agrupado(:b_periodo) LOOP
        SELECT SUM(aporte_sbif)
        INTO v_reg_res.aporte_total
        FROM DETALLE_APORTE_SBIF
        WHERE TO_CHAR(fecha_transaccion, 'MMYYYY') = r_res.mes_anno
          AND tipo_transaccion = r_res.nombre_tptran_tarjeta;

        -- Se realiza una inserción simplificada de los datos. 
        -- Los datos se insertan directamente en el registro o valores por posición.

        INSERT INTO RESUMEN_APORTE_SBIF VALUES (
            r_res.mes_anno,
            r_res.nombre_tptran_tarjeta,
            r_res.suma_monto,
            v_reg_res.aporte_total
        );
    END LOOP;

    -- Se confirma la transacción siempre y cuando todo esté correcto.

    IF v_iteraciones = v_total_esperado THEN
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Proceso Exitoso. Registros: ' || v_iteraciones);
    ELSE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error de consistencia. Rollback aplicado.');
    END IF;

EXCEPTION  -- Se incorpora otro manejo de excepciones conforme a lo solicitado en el caso.
    WHEN e_error_pk THEN
        DBMS_OUTPUT.PUT_LINE('Error: Llave primaria duplicada.');
        ROLLBACK;
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        ROLLBACK;
END;
/