
########################################################################
class TradeUtility(object):
    """盘口交易的辅助函数"""
    #----------------------------------------------------------------------
    @classmethod 
    def buyByTickShotData(self,tick,targetPosition,priceDeviation):
        #tick数据包括['S1','S2','S3','S4','S5','S6','S7','S8','S9','S10','B1','B2','B3','B4','B5','B6','B7','B8','B9','B10','SV1','SV2','SV3','SV4','SV5','SV6','SV7','SV8','SV9','SV10','BV1','BV2','BV3','BV4','BV5','BV6','BV7','BV8','BV9','BV10']
        priceIndex=[0,1,2,3,4,5,6,7,8,9]
        volumeIndex=[20,21,22,23,24,25,26,27,28,29]
        ceilPrice=tick[priceIndex[0]]*(1+priceDeviation)
        buyPosition=0
        buyAmount=0
        for i in range(len(priceIndex)):
            price=tick[priceIndex[i]]
            volume=tick[volumeIndex[i]]
            if ((price<ceilPrice) & (targetPosition>0)):
                buyVolume=min(targetPosition,volume)
                buyPosition=buyPosition+buyVolume
                buyAmount=buyAmount+buyVolume*price
                targetPosition=targetPosition-buyVolume
                pass
            pass
        pass
        averagePrice=buyAmount/buyPosition
        return [averagePrice,buyPosition,buyAmount]
    #----------------------------------------------------------------------
    @classmethod 
    def sellByTickShotData(self,tick,targetPosition,priceDeviation):
        #tick数据包括['S1','S2','S3','S4','S5','S6','S7','S8','S9','S10','B1','B2','B3','B4','B5','B6','B7','B8','B9','B10','SV1','SV2','SV3','SV4','SV5','SV6','SV7','SV8','SV9','SV10','BV1','BV2','BV3','BV4','BV5','BV6','BV7','BV8','BV9','BV10']
        priceIndex=[10,11,12,13,14,15,16,17,18,19]
        volumeIndex=[30,31,32,33,34,35,36,37,38,39]
        floorPrice=tick[priceIndex[0]]*(1-priceDeviation)
        sellPosition=0
        sellAmount=0
        for i in range(len(priceIndex)):
            price=tick[priceIndex[i]]
            volume=tick[volumeIndex[i]]
            if ((price>floorPrice) & (targetPosition>0)):
                sellVolume=min(targetPosition,volume)
                sellPosition=sellPosition+sellVolume
                sellAmount=sellAmount+sellVolume*price
                targetPosition=targetPosition-sellVolume
                pass
            pass
        pass
        averagePrice=sellAmount/sellPosition
        return [averagePrice,sellPosition,sellAmount]
########################################################################