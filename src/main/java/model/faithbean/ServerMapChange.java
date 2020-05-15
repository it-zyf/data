package model.faithbean;


import Base.OtherBaseEntity;

/**
 * 进出副本日志
 */
public class ServerMapChange extends OtherBaseEntity {
    public	String	logName;
    public	String	mapTemplateId;
    public	Integer	mapType;
    public	Integer	mapSubType;
    public	Integer	changeType;
    public	Integer	stateType;

}
