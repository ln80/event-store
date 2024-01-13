package dynamodb

import (
	"reflect"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// func aggregateCapacity(c1 types.ConsumedCapacity, cs ...types.ConsumedCapacity) types.ConsumedCapacity {
// 	if len(cs) == 0 {
// 		return c1
// 	}

// 	for _, c := range cs {
// 		if c1.CapacityUnits != nil && c.CapacityUnits != nil {
// 			*c1.CapacityUnits += *c.CapacityUnits
// 		}
// 		if c1.WriteCapacityUnits != nil && c.WriteCapacityUnits != nil {
// 			*c1.WriteCapacityUnits += *c.WriteCapacityUnits
// 		}
// 		if c1.ReadCapacityUnits != nil && c.ReadCapacityUnits != nil {
// 			*c1.ReadCapacityUnits += *c.ReadCapacityUnits
// 		}

// 		if c1.Table != nil && c.Table != nil {
// 			if c1.Table.CapacityUnits != nil && c.Table.CapacityUnits != nil {
// 				*c1.CapacityUnits += *c.CapacityUnits
// 			}
// 			if c1.Table.WriteCapacityUnits != nil && c.Table.WriteCapacityUnits != nil {
// 				*c1.Table.WriteCapacityUnits += *c.Table.WriteCapacityUnits
// 			}
// 			if c1.Table.ReadCapacityUnits != nil && c.Table.ReadCapacityUnits != nil {
// 				*c1.Table.ReadCapacityUnits += *c.Table.ReadCapacityUnits
// 			}
// 		}
// 	}

// 	return c1
// }

type ConsumedCapacity struct {
	Total      float64
	Read       float64
	Write      float64
	GSI        map[string]float64
	GSIRead    map[string]float64
	GSIWrite   map[string]float64
	LSI        map[string]float64
	LSIRead    map[string]float64
	LSIWrite   map[string]float64
	Table      float64
	TableRead  float64
	TableWrite float64
	TableName  string
}

func (cc *ConsumedCapacity) IsZero() bool {
	return cc == nil || reflect.DeepEqual(*cc, ConsumedCapacity{})
}

func addConsumedCapacity(cc *ConsumedCapacity, raw *types.ConsumedCapacity) {
	if cc == nil || raw == nil {
		return
	}
	if raw.CapacityUnits != nil {
		cc.Total += *raw.CapacityUnits
	}
	if raw.ReadCapacityUnits != nil {
		cc.Read += *raw.ReadCapacityUnits
	}
	if raw.WriteCapacityUnits != nil {
		cc.Write += *raw.WriteCapacityUnits
	}
	if len(raw.GlobalSecondaryIndexes) > 0 {
		if cc.GSI == nil {
			cc.GSI = make(map[string]float64, len(raw.GlobalSecondaryIndexes))
		}
		for name, consumed := range raw.GlobalSecondaryIndexes {
			cc.GSI[name] = cc.GSI[name] + *consumed.CapacityUnits
			if consumed.ReadCapacityUnits != nil {
				if cc.GSIRead == nil {
					cc.GSIRead = make(map[string]float64, len(raw.GlobalSecondaryIndexes))
				}
				cc.GSIRead[name] = cc.GSIRead[name] + *consumed.ReadCapacityUnits
			}
			if consumed.WriteCapacityUnits != nil {
				if cc.GSIWrite == nil {
					cc.GSIWrite = make(map[string]float64, len(raw.GlobalSecondaryIndexes))
				}
				cc.GSIWrite[name] = cc.GSIWrite[name] + *consumed.WriteCapacityUnits
			}
		}
	}
	if len(raw.LocalSecondaryIndexes) > 0 {
		if cc.LSI == nil {
			cc.LSI = make(map[string]float64, len(raw.LocalSecondaryIndexes))
		}
		for name, consumed := range raw.LocalSecondaryIndexes {
			cc.LSI[name] = cc.LSI[name] + *consumed.CapacityUnits
			if consumed.ReadCapacityUnits != nil {
				if cc.LSIRead == nil {
					cc.LSIRead = make(map[string]float64, len(raw.LocalSecondaryIndexes))
				}
				cc.LSIRead[name] = cc.LSIRead[name] + *consumed.ReadCapacityUnits
			}
			if consumed.WriteCapacityUnits != nil {
				if cc.LSIWrite == nil {
					cc.LSIWrite = make(map[string]float64, len(raw.LocalSecondaryIndexes))
				}
				cc.LSIWrite[name] = cc.LSIWrite[name] + *consumed.WriteCapacityUnits
			}
		}
	}
	if raw.Table != nil {
		cc.Table += *raw.Table.CapacityUnits
		if raw.Table.ReadCapacityUnits != nil {
			cc.TableRead += *raw.Table.ReadCapacityUnits
		}
		if raw.Table.WriteCapacityUnits != nil {
			cc.TableWrite += *raw.Table.WriteCapacityUnits
		}
	}
	if raw.TableName != nil {
		cc.TableName = *raw.TableName
	}
}
