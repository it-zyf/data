package com.example.xyy.test.bean;

import Base.OtherBaseEntity;

/**
 * 击杀怪物日志
 */
public class ServerBossKill extends OtherBaseEntity {
    public String bossId;
    public String mapId;
    public Integer playerNum;
    public Long battleBeginTime;
    public Long battleEndTime;
    public Long battleContinuedTime;

}
