package demo.entity;

import java.util.List;

public class BaseGraph {
    String sid;
    String sname;
    List<ChampionAttri> championAttriList;

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getSname() {
        return sname;
    }

    public void setSname(String sname) {
        this.sname = sname;
    }

    public List<ChampionAttri> getChampionAttriList() {
        return championAttriList;
    }

    public void setChampionAttriList(List<ChampionAttri> championAttriList) {
        this.championAttriList = championAttriList;
    }
}
