package model.faithbean;


import Base.OtherBaseEntity;

/**
 * <p>
 * 宝藏寻宝
 * </p>
 *
 * @author twg
 * @since 2020-03-26
 */
public class ServerLuckyDraw extends OtherBaseEntity {


    public String logName;
    public String itemId;
    public Integer itemNum;
    public Integer treasureType;
    public Integer costType;
    public Integer isFirstTimeDo;
    public String oldLuckyValue;
    public String newLuckyValue;
    public Integer isFree;

}
