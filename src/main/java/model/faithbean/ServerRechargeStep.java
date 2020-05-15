package model.faithbean;


import Base.OtherBaseEntity;

/**
 * <p>
 * 充值步骤
 * </p>
 *
 * @author twg
 * @since 2020-03-26
 */
public class ServerRechargeStep extends OtherBaseEntity {

    public String logName;
    public String orderId;
    public String goodsId;
    public Integer stepNum;
}
