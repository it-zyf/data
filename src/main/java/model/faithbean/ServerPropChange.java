package model.faithbean;


import Base.OtherBaseEntity;

/**
 * <p>
 * 道具变更
 * </p>
 *
 * @author twg
 * @since 2020-03-26
 */
public class ServerPropChange extends OtherBaseEntity {
    public	String	logName;
    public	String	propId;
    public	Integer	changeType;
    public	Integer	changeDescribe;
    public	Integer	changeCount;
    public	Integer	residueCount;

}
