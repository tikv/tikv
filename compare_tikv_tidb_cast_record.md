## RPN 部分

- 注：`uint_to_*`的函数会在PR被更新https://github.com/tikv/tikv/pull/5179/files，其直接实现，已经基于tidb实现校对过几次了，而不是直接用ToInt trait等的trait来实现。所以这里比较的是`to_uint`等函数可能并没有被用到

### to int

- int to int, 相同，不过更慢，因为tidb的不需要去查上下界（等同于i64的上下界，所以不查上下界然后比较也没有问题）
- uint to int，这个PR（https://github.com/tikv/tikv/pull/5179/files）合了后一致，现在不一致
   ```rust
   // tikv，一旦大于signed的上界，就报错，而tidb是直接cast 
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        let upper_bound = integer_signed_upper_bound(tp);
        if *self > upper_bound as u64 {
            ctx.handle_overflow_err(overflow!(self, upper_bound))?;
            return Ok(upper_bound);
        }
        Ok(*self as i64)
    }
   ```
   ```go
    func (b *builtinCastIntAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
    	res, isNull, err = b.args[0].EvalInt(b.ctx, row)
    	if isNull || err != nil {
    		return
    	}
    	if b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag) && res < 0 {
    		res = 0
    	}
    	return
    }
   ```
- float to int，不一致
  ```rust
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        let val = (*self).round();
        let lower_bound = integer_signed_lower_bound(tp);
        if val < lower_bound as f64 {
            ctx.handle_overflow_err(overflow!(val, lower_bound))?;
            return Ok(lower_bound);
        }

        // here, two of them is different.
        let upper_bound = integer_signed_upper_bound(tp);
        if val > upper_bound as f64 {
            ctx.handle_overflow_err(overflow!(val, upper_bound))?;
            return Ok(upper_bound);
        }
        Ok(val as i64)
    }
  ```
  ```go
    func ConvertFloatToInt(fval float64, lowerBound, upperBound int64, tp byte) (int64, error) {
    	val := RoundFloat(fval)
    	if val < float64(lowerBound) {
    		return lowerBound, overflow(val, tp)
    	}
    
    	if val >= float64(upperBound) {
    		if val == float64(upperBound) {
    			return upperBound, nil
    		}
    		return upperBound, overflow(val, tp)
    	}
    	return int64(val), nil
    }
  ```
- decimal to int，可能不一致——`round`、`dec.to_int`是否一致、`val.to_int`是否有必要？
   ```rust
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        // TODO: avoid this clone
        let dec = round_decimal_with_ctx(ctx, self.clone())?;
        let val = dec.as_i64_with_ctx(ctx)?;
        val.to_int(ctx, tp)
    }
   ```
   ```go
   // ...太长，略
   // builtinCastDecimalAsIntSig::evalInt
   ```
- string to int，需要更多工作，TODO
- time to int，等待liming的重构time完成，TODO
- duration to int，等待https://github.com/tikv/tikv/pull/5199合并，TODO
- json to int，等待https://github.com/tikv/tikv/pull/5199合并，TODO

### to uint

- int to uint，与TiDB的不一致
   ```rust
   // tikv，有should_clip_to_zero，而tidb是直接cast。并且tidb有检查是否in_union
    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64> {
        if *self < 0 && ctx.should_clip_to_zero() {
            ctx.handle_overflow_err(overflow!(self, 0))?;
            return Ok(0);
        }

        let upper_bound = integer_unsigned_upper_bound(tp);
        if *self as u64 > upper_bound {
            ctx.handle_overflow_err(overflow!(self, upper_bound))?;
            return Ok(upper_bound);
        }
        Ok(*self as u64)
    }
   ```
   ```go
   // tidb
    func (b *builtinCastIntAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
    	res, isNull, err = b.args[0].EvalInt(b.ctx, row)
    	if isNull || err != nil {
    		return
    	}
    	if b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag) && res < 0 {
    		res = 0
    	}
    	return
    }
   ```
   补充一个
   ```sql
    use test;
    
    drop table if exists tb1;
    create table tb1
    (
        a bigint(64) signed,
        b bigint(64) unsigned
    );
    select *
    from tb1;
    insert into tb1 (b)
    select cast(-1 as unsigned int);
    
    insert into tb1 (a, b)
    values (-1, 10);
    
    insert into tb1(b)
    select a
    from tb1;
   ```
   最后一句执行失败，报错`Data truncation: constant -1 overflows bigint`，但是断点下在`builtinCastIntAsIntSig::evalInt`没中断

- uint to uint，一致，但是更慢，因为tidb是直接cast，注意，`cast_uint_as_uint`应该在这个PR（https://github.com/tikv/tikv/pull/5179/files）被实现，（然而并没有，所以这里的`to_uint`虽然与TiDB的不一致，但是似乎问题不大？然而，看起来并不需要ad-hoc地实现`uint_to_uint`，而是应该用这里这个函数）
   ```rust
    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64> {
        let upper_bound = integer_unsigned_upper_bound(tp);
        if *self > upper_bound {
            ctx.handle_overflow_err(overflow!(self, upper_bound))?;
            return Ok(upper_bound);
        }
        Ok(*self)
    }
    ```
   ```go
   // tidb
    func (b *builtinCastIntAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
    	res, isNull, err = b.args[0].EvalInt(b.ctx, row)
    	if isNull || err != nil {
    		return
    	}
    	if b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag) && res < 0 {
    		res = 0
    	}
    	return
    }
   ```
- float to uint，
- decimal to uint，可能不一致，需要深入看TiDB、TiKV的decimal的实现`round`和`decimal_as_u64`的实现，并且错误处理也不一样
   ```go
   // 在TiKV的实现中没有搜到decimal用`Truncated incorrect..`这个错误
	if types.ErrOverflow.Equal(err) {
		warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", val)
		err = b.ctx.GetSessionVars().StmtCtx.HandleOverflow(err, warnErr)
	}
   ```

- string to uint，同样需要更深入的分析，两边长得很不一样
- time、duration、json要等PR合了才能分析


### to string

- int to string，不一致，还有ProduceStrWithSpecifiedTp和padzero需要添加进去
- uint to string，这个PR（https://github.com/tikv/tikv/pull/5179/files）中与TIDB的一样
- float to string，TiKV有`impl<T> ConvertTo<String> for T where T: ToString + Evaluable`，但是只有`Real`是实现了`Evaluable`，`f64`没有。并且也是缺少`ProduceStrWithSpecifiedTp`和`padZeroForBinaryType`，并且bit长度的匹配也是没有。另外，rust的i64什么的tostring与golang的format一样吗？与MySQL的一样吗？
- string to string，同样少了`ProduceStrWithSpecifiedTp`和`padZeroForBinaryType`
- time to string，等liming重构完time
- duration to String：需要校对duration与TiDB的duraion的ToString实现，同样少了`ProduceStrWithSpecifiedTp`和`padZeroForBinaryType`
- json to string：需要校对json的tostring与TiDB的是否一致，并且有另一个问题，json to string是用这个trait实现的，所以简单地在这里加上`ProduceStrWithSpecifiedTp`和`padZeroForBinaryType`会导致json to string与TiDB的不一致
   ```rust
    impl<T> ConvertTo<String> for T
    where
        T: ToString + Evaluable,
    {
        #[inline]
        fn convert(&self, _: &mut EvalContext) -> Result<String> {
            // FIXME: There is an additional step `ProduceStrWithSpecifiedTp` in TiDB.
            Ok(self.to_string())
        }
    }
   ```

### TODO


## ScalarFunc部分

- 实现时，已经做了很多一致性的工作，反复校对了几次，主要问题应该在convert triat的实现上

## bugs

- TiDB有多个疑似Bug，需要复现与修复

## 其他

- 建议再校对一次`produce_str_with_specified_tp`和`pad_zero_for_binary_type`和`produce_dec_with_specified_tp`，虽然实现时已经校对了几次




## 修复过程

#### 假设

- Duration、Decimal、JSON的API的行为与TiDB的一致
- `get_valid_int_prefix`、rust的str parse to int、uint 与golang的`strconv.ParseUint`, rust 的 f64 to string 之类与golang的相同
- rust  



#### 问题

- 错误处理不是非常相同，都有标准FIXME或是TODO
- 



## TiDB的问题

- 注释了TODO的那些
- AsDuration
   ```go
    func (b *builtinCastJSONAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
    	val, isNull, err := b.args[0].EvalJSON(b.ctx, row)
    	if isNull || err != nil {
    		return res, isNull, err
    	}
    	res, err = types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, s, int8(b.tp.Decimal))
    	if types.ErrTruncatedWrongVal.Equal(err) {
    		err = b.ctx.GetSessionVars().StmtCtx.HandleTruncate(err)
    	}
    	return
    }
   ```
   ```go
    func (b *builtinCastDecimalAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
    	val, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
    	if isNull || err != nil {
    		return res, true, err
    	}
    	res, err = types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, string(val.ToString()), int8(b.tp.Decimal))
    	if types.ErrTruncatedWrongVal.Equal(err) {
    		err = b.ctx.GetSessionVars().StmtCtx.HandleTruncate(err)
    		// ZeroDuration of error ErrTruncatedWrongVal needs to be considered NULL.
    		if res == types.ZeroDuration {
    			return res, true, err
    		}
    	}
    	return res, false, err
    }
    ```
    ```go
    func (b *builtinCastStringAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
    	val, isNull, err := b.args[0].EvalString(b.ctx, row)
    	if isNull || err != nil {
    		return res, isNull, err
    	}
    	res, err = types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, val, int8(b.tp.Decimal))
    	if types.ErrTruncatedWrongVal.Equal(err) {
    		err = b.ctx.GetSessionVars().StmtCtx.HandleTruncate(err)
    		// ZeroDuration of error ErrTruncatedWrongVal needs to be considered NULL.
    		if res == types.ZeroDuration {
    			return res, true, err
    		}
    	}
    	return res, false, err
    }
    ```
    三者的不同在于`res == types.ZeroDuration`时的不同。TiKV中的`Duration::parse`如果返回err，则无res返回，所以我们无法判断是否是ZeroDuration。按照duration作者的说法（dengliming），`Duration::Parse`如果返回错误则不可能返回有效的值

