
########################################################################
class TradeUtility(object):
    """盘口交易的辅助函数"""
    #----------------------------------------------------------------------
    @classmethod 
    def buyByTickShotData(self,tick,targetPosition,priceDeviation):
        #tick数据包括['S1', 'S2', 'S3', 'S4', 'S5', 'B1','B2', 'B3', 'B4', 'B5', 'SV1', 'SV2', 'SV3', 'SV4', 'SV5', 'BV1', 'BV2','BV3', 'BV4', 'BV5']
        priceIndex=[0,1,2,3,4]
        volumeIndex=[10,11,12,13,14]
        ceilPrice=tick[priceIndex[0]]*(1+priceDeviation)
        buyPosition=0
        buyAmount=0
        for i in range(5):
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
        #tick数据包括['S1', 'S2', 'S3', 'S4', 'S5', 'B1','B2', 'B3', 'B4', 'B5', 'SV1', 'SV2', 'SV3', 'SV4', 'SV5', 'BV1', 'BV2','BV3', 'BV4', 'BV5']
        priceIndex=[5,6,7,8,9]
        volumeIndex=[15,16,17,18,19]
        floorPrice=tick[priceIndex[0]]*(1-priceDeviation)
        sellPosition=0
        sellAmount=0
        for i in range(5):
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