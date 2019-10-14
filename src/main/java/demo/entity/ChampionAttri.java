package demo.entity;

import java.util.List;

public class ChampionAttri {
    private String champion_name;
    private Long championID;
    private List<PosAttri> posAttriList;

    public ChampionAttri(String champion_name, Long championID, List<PosAttri> posAttriList) {
        this.champion_name = champion_name;
        this.championID = championID;
        this.posAttriList = posAttriList;
    }

    public String getChampion_name() {
        return champion_name;
    }

    public void setChampion_name(String champion_name) {
        this.champion_name = champion_name;
    }

    public Long getChampionID() {
        return championID;
    }

    public void setChampionID(Long championID) {
        this.championID = championID;
    }

    public List<PosAttri> getPosAttriList() {
        return posAttriList;
    }

    public void setPosAttriList(List<PosAttri> posAttriList) {
        this.posAttriList = posAttriList;
    }
}
