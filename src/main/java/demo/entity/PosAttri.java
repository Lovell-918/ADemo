package demo.entity;

public class PosAttri {
    private String pos;
    private double metric;
    private Long chamPoID;


    public PosAttri(String pos, double metric, Long chamPoID) {
        this.pos = pos;
        this.metric = metric;
        this.chamPoID = chamPoID;
    }

    public String getPos() {
        return pos;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }

    public double getMetric() {
        return metric;
    }

    public void setMetric(double metric) {
        this.metric = metric;
    }

    public Long getChamPoID() {
        return chamPoID;
    }

    public void setChamPoID(Long chamPoID) {
        this.chamPoID = chamPoID;
    }
}
