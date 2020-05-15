package model.faithbean;


import Base.OtherBaseEntity;

/**
 * <p>
 * 军团击杀日志
 * </p>
 *
 * @author twg
 * @since 2020-03-26
 */
public class ServerCrossServerBossKill extends OtherBaseEntity {

    public	String	logName;
    public	Integer	bossId;
    public	Integer	killLegionId;
    public	String	killLegionName;
    public	Integer	top1Id;
    public	String	top1Name;
    public	Integer	top2Id;
    public	String	top2Name;
    public	Integer	top3Id;
    public	String	top3Name;


}
