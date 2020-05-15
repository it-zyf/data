package model.faithbean;

import Base.OtherBaseEntity;

/**
 *商城日志
 */
public class ServerRoleMallLog extends OtherBaseEntity {
    public	String	logName;
    public	Integer	activityId;
    public	Integer	rewardId;
    public	Integer	rewardNum;
    public	Integer	consumedCurrencyType;
    public	Integer	consumedNum;
    public	String	storeName;
    public	String	storeId;
    public	Integer	soldNum;
    public	String	soldCurrecyId;
    public	Long	soldNumAmount;

}
