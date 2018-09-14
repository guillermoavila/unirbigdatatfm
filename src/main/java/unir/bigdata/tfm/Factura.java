package unir.bigdata.tfm;

import java.io.Serializable;

public class Factura implements Comparable<Factura>, Serializable {
    private long CODIGO_COMPROBANTE;
    private long FECHA_EMISION;
    private String IDENTIFICACION_COMPRADOR;
    private float TOTAL_SIN_IMPUESTOS;
    private float IMPORTE_TOTAL;
    private float BASE_GRAVADA;
    private float BASE_TARIFA_0;
    private float MONTO_IVA;
    private String NUMERO_RUC_EMISOR;
    private long CONTADOR;
    private long FECHA_RECEPCION_EVENTO;
    private long FECHA_INICIO_PROCESAMIENTO;
    private long FECHA_FIN_PROCESAMIENTO;
    private long TIEMPO_PROCESAMIENTO;

    @Override
    public String toString() {
        return "Factura{" +
                "CODIGO_COMPROBANTE=" + CODIGO_COMPROBANTE +
                ", FECHA_EMISION=" + FECHA_EMISION +
                ", IDENTIFICACION_COMPRADOR='" + IDENTIFICACION_COMPRADOR + '\'' +
                ", TOTAL_SIN_IMPUESTOS=" + TOTAL_SIN_IMPUESTOS +
                ", IMPORTE_TOTAL=" + IMPORTE_TOTAL +
                ", BASE_GRAVADA=" + BASE_GRAVADA +
                ", BASE_TARIFA_0=" + BASE_TARIFA_0 +
                ", MONTO_IVA=" + MONTO_IVA +
                ", NUMERO_RUC_EMISOR='" + NUMERO_RUC_EMISOR + '\'' +
                ", FECHA_RECEPCION_EVENTO=" + FECHA_RECEPCION_EVENTO +
                ", FECHA_INICIO_PROCESAMIENTO=" + FECHA_INICIO_PROCESAMIENTO +
                ", FECHA_FIN_PROCESAMIENTO=" + FECHA_FIN_PROCESAMIENTO +
                ", CONTADOR=" + CONTADOR +
                ", TIEMPO_PROCESAMIENTO=" + getTIEMPO_PROCESAMIENTO() +
                '}';
    }

    public String getResumenTiempos() {
        return FECHA_RECEPCION_EVENTO+":"+
                FECHA_INICIO_PROCESAMIENTO+":"+
                FECHA_FIN_PROCESAMIENTO+":"+
                this.getDEMORA_INICIO_PROCESAMIENTO()+":"+
                this.getTIEMPO_PROCESAMIENTO();
    }

    public long getCODIGO_COMPROBANTE() {
        return CODIGO_COMPROBANTE;
    }

    public void setCODIGO_COMPROBANTE(long CODIGO_COMPROBANTE) {
        this.CODIGO_COMPROBANTE = CODIGO_COMPROBANTE;
    }

    public long getFECHA_EMISION() {
        return FECHA_EMISION;
    }

    public void setFECHA_EMISION(long FECHA_EMISION) {
        this.FECHA_EMISION = FECHA_EMISION;
    }

    public String getIDENTIFICACION_COMPRADOR() {
        return IDENTIFICACION_COMPRADOR;
    }

    public void setIDENTIFICACION_COMPRADOR(String IDENTIFICACION_COMPRADOR) {
        this.IDENTIFICACION_COMPRADOR = IDENTIFICACION_COMPRADOR;
    }

    public float getTOTAL_SIN_IMPUESTOS() {
        return TOTAL_SIN_IMPUESTOS;
    }

    public void setTOTAL_SIN_IMPUESTOS(float TOTAL_SIN_IMPUESTOS) {
        this.TOTAL_SIN_IMPUESTOS = TOTAL_SIN_IMPUESTOS;
    }

    public float getIMPORTE_TOTAL() {
        return IMPORTE_TOTAL;
    }

    public void setIMPORTE_TOTAL(float IMPORTE_TOTAL) {
        this.IMPORTE_TOTAL = IMPORTE_TOTAL;
    }

    public float getBASE_GRAVADA() {
        return BASE_GRAVADA;
    }

    public void setBASE_GRAVADA(float BASE_GRAVADA) {
        this.BASE_GRAVADA = BASE_GRAVADA;
    }

    public float getBASE_TARIFA_0() {
        return BASE_TARIFA_0;
    }

    public void setBASE_TARIFA_0(float BASE_TARIFA_0) {
        this.BASE_TARIFA_0 = BASE_TARIFA_0;
    }

    public float getMONTO_IVA() {
        return MONTO_IVA;
    }

    public void setMONTO_IVA(float MONTO_IVA) {
        this.MONTO_IVA = MONTO_IVA;
    }

    public String getNUMERO_RUC_EMISOR() {
        return NUMERO_RUC_EMISOR;
    }

    public void setNUMERO_RUC_EMISOR(String NUMERO_RUC_EMISOR) {
        this.NUMERO_RUC_EMISOR = NUMERO_RUC_EMISOR;
    }

    @Override
    public int compareTo(Factura o) {
        if(o==null) {
            return 1;
        }else if(o.getCODIGO_COMPROBANTE() != CODIGO_COMPROBANTE) {
            return 1;
        }else
            return 0;
    }

    public long getFECHA_INICIO_PROCESAMIENTO() {
        return FECHA_INICIO_PROCESAMIENTO;
    }

    public void setFECHA_INICIO_PROCESAMIENTO(long FECHA_INICIO_PROCESAMIENTO) {
        this.FECHA_INICIO_PROCESAMIENTO = FECHA_INICIO_PROCESAMIENTO;
    }

    public long getFECHA_FIN_PROCESAMIENTO() {
        return FECHA_FIN_PROCESAMIENTO;
    }

    public void setFECHA_FIN_PROCESAMIENTO(long FECHA_FIN_PROCESAMIENTO) {
        this.FECHA_FIN_PROCESAMIENTO = FECHA_FIN_PROCESAMIENTO;
    }

    public long getTIEMPO_PROCESAMIENTO() {
        return this.getFECHA_FIN_PROCESAMIENTO() - this.getFECHA_INICIO_PROCESAMIENTO();
    }

    public long getDEMORA_INICIO_PROCESAMIENTO() {
        return this.FECHA_INICIO_PROCESAMIENTO - this.FECHA_RECEPCION_EVENTO;
    }

    public long getFECHA_RECEPCION_EVENTO() {
        return FECHA_RECEPCION_EVENTO;
    }

    public void setFECHA_RECEPCION_EVENTO(long FECHA_RECEPCION_EVENTO) {
        this.FECHA_RECEPCION_EVENTO = FECHA_RECEPCION_EVENTO;
    }

    public long getCONTADOR() {
        return CONTADOR;
    }

    public void setCONTADOR(long CONTADOR) {
        this.CONTADOR = CONTADOR;
    }

    public void setTIEMPO_PROCESAMIENTO(long TIEMPO_PROCESAMIENTO) {
        this.TIEMPO_PROCESAMIENTO = TIEMPO_PROCESAMIENTO;
    }

}
