package demo.entity;

public class PosAttri {
    private String pos;
    private double metric;

    public PosAttri(String pos, double metric) {
        this.pos = pos;
        this.metric = metric;
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

}
