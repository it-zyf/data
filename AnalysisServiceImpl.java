package com.umi.ga.service.clientInterface.impl;

import com.comb.framework.db.annotation.DbSwitch;
import com.umi.ga.analysis.dao.PlayerAnalysisDao;
import com.umi.ga.analysis.model.PlayerAnalysis;
import com.umi.ga.common.HttpResult;
import com.umi.ga.entitys.*;
import com.umi.ga.service.clientInterface.AnalysisService;
import com.umi.ga.utils.ArgumentsConver;
import com.umi.ga.utils.BaseResultUtil;
import com.umi.ga.utils.DateHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * @Author guowenchuang
 * @Date 2020/4/17 16:39
 */
@Service
@DbSwitch(master = {"master"}, slave = {"hivemaster"}, constraint = false)
public class AnalysisServiceImpl implements AnalysisService {

    @Autowired
    private PlayerAnalysisDao playerAnalysisDao;

    /**
     * 玩家充值排行
     *
     * @param flag
     * @return
     */
    @Override
    public HttpResult hiveRechargeRanking(Flag flag) {
        List<PlayerAnalysis> rankList = new ArrayList<>();
        List<PlayerAnalysis> rankList2 = new ArrayList<>();
        flag = ArgumentsConver.converServerIdAndChannelId(flag);
        int count = 0;
        try {
            rankList = playerAnalysisDao.queryRechargeRanking(flag);
            count = null != rankList ? rankList.size() : 0;
            if (null != rankList && 0 != rankList.size())
                for (int i = Math.min(count, flag.getFromIndex()); i < Math.min(count, flag.getToIndex()); i++)
                    rankList2.add(rankList.get(i));
        } catch (Exception e) {
            e.printStackTrace();
            return BaseResultUtil.resultFail("查询失败", rankList2, null, count);
        }
        return BaseResultUtil.resultSuccess("查询成功", rankList2, null, count);
    }

    /**
     * 等级流失
     *
     * @param flag
     * @return
     */
    @Override
    public HttpResult hiveLevelAway(Flag flag) {
        List<Flag> levelList = new ArrayList<>();
        List<Flag> levelList2 = new ArrayList<>();
        String now = DateHelper.dateSimpleFormatString(new Date());
        flag.setEndTimeSecond(now);
        flag.setStartTimeSecond(DateHelper.dateIncrease(now, Long.valueOf(-6)));
        flag = ArgumentsConver.converServerIdAndChannelId(flag);
        int count = 0;
        try {
            levelList = playerAnalysisDao.hiveLevelAway(flag);
            count = null != levelList ? levelList.size() : 0;
            if (null != levelList && 0 != levelList.size())
                for (int i = Math.min(count, flag.getFromIndex()); i < Math.min(count, flag.getToIndex()); i++)
                    levelList2.add(levelList.get(i));
        } catch (Exception e) {
            e.printStackTrace();
            return BaseResultUtil.resultFail("查询失败", levelList2, null, count);
        }
        return BaseResultUtil.resultSuccess("查询成功", levelList2, null, count);
    }

    /**
     * 全部货币类型
     *
     * @return
     */
    @Override
    public HttpResult hiveQueryCurrencyType() {
        List<CurrencyData> typeList = new ArrayList<>();
        try {
            typeList = playerAnalysisDao.hiveQueryCurrencyType();
        } catch (Exception e) {
            e.printStackTrace();
            return BaseResultUtil.resultFail("查询失败", typeList);
        }
        return BaseResultUtil.resultSuccess("查询成功", typeList);
    }

    /**
     * 货币产出与消耗数据
     *
     * @param flag
     * @return
     */
    @Override
    public HttpResult hiveQueryCurrency(Flag flag) {
        List<CurrencyData> currencyList = new ArrayList<>();
        List<CurrencyData> currencyDataList = new ArrayList<>();
        flag = ArgumentsConver.converServerIdAndChannelId(flag);
        int count = 0;
        try {
            currencyList = playerAnalysisDao.hiveQueryCurrency(flag);
            count = null != currencyList ? currencyList.size() : 0;
            if (null != currencyList && 0 != currencyList.size())
                for (int i = Math.min(count, flag.getFromIndex()); i < Math.min(count, flag.getToIndex()); i++)
                    currencyDataList.add(currencyList.get(i));

        } catch (Exception e) {
            e.printStackTrace();
            return BaseResultUtil.resultFail("查询失败", currencyDataList, null, count);
        }
        return BaseResultUtil.resultSuccess("查询成功", currencyDataList, null, count);
    }

    /**
     * 物品产出与消耗数据
     *
     * @param flag
     * @return
     */
    @Override
    public HttpResult hiveQueryGoods(Flag flag) {
        List<Flag> goodsList = new ArrayList<>();
        List<Flag> goodsList2 = new ArrayList<>();
        flag = ArgumentsConver.converServerIdAndChannelId(flag);
        int count = 0;
        try {
            goodsList = playerAnalysisDao.hiveQueryGoods(flag);
            count = null != goodsList ? goodsList.size() : 0;
            if (null != goodsList && 0 != goodsList.size())
                for (int i = Math.min(count, flag.getFromIndex()); i < Math.min(count, flag.getToIndex()); i++)
                    goodsList2.add(goodsList.get(i));
        } catch (Exception e) {
            e.printStackTrace();
            return BaseResultUtil.resultFail("查询失败", goodsList2, null, count);
        }
        return BaseResultUtil.resultSuccess("查询成功", goodsList2, null, count);
    }

    /**
     * 全部商店类型
     *
     * @return
     */
    @Override
    public HttpResult hiveQueryStoreType() {
        List<MallLog> typeList = new ArrayList<>();
        try {
            typeList = playerAnalysisDao.hiveQueryStoreType();
        } catch (Exception e) {
            e.printStackTrace();
            return BaseResultUtil.resultFail("查询失败", typeList);
        }
        return BaseResultUtil.resultSuccess("查询成功", typeList);
    }

    /**
     * 商城购买
     *
     * @param flag
     * @return
     */
    @Override
    public HttpResult hiveQueryMallLog(Flag flag) {
        List<MallLog> mallList = new ArrayList<>();
        List<MallLog> mallList2 = new ArrayList<>();
        flag = ArgumentsConver.converServerIdAndChannelId(flag);
        int count = 0;
        try {
            mallList = playerAnalysisDao.hiveQueryMallLog(flag);
            count = null != mallList ? mallList.size() : 0;
            if (null != mallList && 0 != mallList.size())
                for (int i = Math.min(count, flag.getFromIndex()); i < Math.min(count, flag.getToIndex()); i++)
                    mallList2.add(mallList.get(i));
        } catch (Exception e) {
            e.printStackTrace();
            return BaseResultUtil.resultFail("查询失败", mallList2, null, count);
        }
        return BaseResultUtil.resultSuccess("查询成功", mallList2, null, count);
    }

    /**
     * 军团列表
     *
     * @param flag
     * @return
     */
    @Override
    public HttpResult hiveQueryLegionLog(Flag flag) {
        List<LegionLog> legionList = new ArrayList<>();
        List<LegionLog> legionList2 = new ArrayList<>();
        flag = ArgumentsConver.converServerIdAndChannelId(flag);
        int count = 0;
        try {
            legionList = playerAnalysisDao.hiveQueryLegionLog(flag);
            count = null != legionList ? legionList.size() : 0;
            if (null != legionList && 0 != legionList.size())
                for (int i = Math.min(count, flag.getFromIndex()); i < Math.min(count, flag.getToIndex()); i++)
                    legionList2.add(legionList.get(i));
        } catch (Exception e) {
            e.printStackTrace();
            return BaseResultUtil.resultFail("查询失败", legionList2, null, count);
        }
        return BaseResultUtil.resultSuccess("查询成功", legionList2, null, count);
    }

    /**
     * 活跃玩家数据
     *
     * @param flag
     * @return
     */
    @Override
    public HttpResult hiveQueryActivePlayer(Flag flag) {
        List<PlayerAnalysis> activeList = new ArrayList<>();
        List<PlayerAnalysis> activeList2 = new ArrayList<>();
        String now = DateHelper.dateSimpleFormatString(new Date());
        flag.setEndTimeSecond(now);
        flag.setStartTimeSecond(DateHelper.dateIncrease(now, Long.valueOf(-6)));
        flag = ArgumentsConver.converServerIdAndChannelId(flag);
        int count = 0;
        try {
            activeList = playerAnalysisDao.hiveQueryActivePlayer(flag);
            count = null != activeList ? activeList.size() : 0;
            if (null != activeList && 0 != activeList.size())
                for (int i = Math.min(count, flag.getFromIndex()); i < Math.min(count, flag.getToIndex()); i++)
                    activeList2.add(activeList.get(i));
        } catch (Exception e) {
            e.printStackTrace();
            return BaseResultUtil.resultFail("查询失败", activeList2, null, count);
        }
        return BaseResultUtil.resultSuccess("查询成功", activeList2, null, count);
    }

    /**
     * 查询全部vip等级
     *
     * @return
     */
    @Override
    public HttpResult hiveQueryVipLevel() {

        List<VipRetain> typeList = new ArrayList<>();
        try {
            typeList = playerAnalysisDao.hiveQueryVipLevel();
            Iterator<VipRetain> iterator = typeList.iterator();
            while (iterator.hasNext()) {
                VipRetain next = iterator.next();
                if ("-1".equals(next.getVipLevel()) || "-1" == next.getVipLevel()) iterator.remove();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return BaseResultUtil.resultFail("查询失败", typeList);
        }
        return BaseResultUtil.resultSuccess("查询成功", typeList);
    }

    /**
     * 付费玩家留存
     *
     * @param flag
     * @return
     */
    @Override
    public HttpResult hiveQueryVipRetain(Flag flag) {
        List<VipRetain> vipList = new ArrayList<>();
        List<VipRetain> vipList2 = new ArrayList<>();
        flag = ArgumentsConver.converServerIdAndChannelId(flag);
        String typeId = "";
        try {
            if (null != flag.getType() && !"".equals(flag.getType())) {
                String[] type = flag.getType().split(",");
                for (String str : type)
                    typeId = typeId + ",\"" + str + "\"";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        flag.setType(typeId.replaceFirst(",", ""));
        int count = 0;
        try {
            vipList = playerAnalysisDao.hiveQueryVipRetain(flag);
            count = null != vipList ? vipList.size() : 0;
            if (null != vipList && 0 != vipList.size())
                for (int i = Math.min(count, flag.getFromIndex()); i < Math.min(count, flag.getToIndex()); i++) {
                    if (null != vipList.get(i).getServerId() && !"".equals(vipList.get(i).getChannelId())
                            && null != vipList.get(i).getChannelId() && !"".equals(vipList.get(i).getChannelId())
                            && !"-1".equals(vipList.get(i).getVipLevel()) && "-1" != vipList.get(i).getVipLevel()
                    )
                        vipList2.add(vipList.get(i));
                }
        } catch (Exception e) {
            e.printStackTrace();
            return BaseResultUtil.resultFail("查询失败", vipList2, null, count);
        }
        return BaseResultUtil.resultSuccess("查询成功", vipList2, null, count);
    }

}
