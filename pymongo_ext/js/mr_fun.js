/**
 * some js functions used by mongodb mapreduce  
 */

//////////////////////////////////////////////////////////////////////////////////////////////////////
function MapOrphans () {  
	//	just finds how many documents exists with given field in each collection 
	//	exaple scope= {'phase':1}  phase =1 on first Map call, =2 on second 
	var emkey= this.%s  // s   it will be replaced by caller with exact field name
	if (phase==1) {var vl={A:1,B:0,sum:1};}  
	else          {var vl={A:0,B:1,sum:1};} 
	emit(emkey, vl);
} 

function ReduceOrphans(key, values) {
	   var result={A:0,B:0,sum:0}; 
	   for(var i in values) {
		  result.A += values[i].A;  
		  result.B += values[i].B; 
		  result.sum += values[i].sum;
	   	}  
	   return result;
	}  
///////////////////////////////////////////////////////////////////////////////////////////////////////
function MapJoin () {  
	//	just finds how many documents exists with given field in each collection 
	//	exaple scope= {'phase':1}  phase =1 on first Map call, =2 on second 
	var emkey= this.%s  // s   it will be replaced by caller with exact field name
	if (phase==1) {var vl={A:this,B:null};}  
	else          {var vl={A:null,B:this};} 
	emit(emkey, vl);
} 

function ReduseJoin(key, values) {
	   var result={A:null,B:null,sum:0}; 
	   for(var i in values) { 
		  result.A = values[i].A;  
		  result.B = values[i].B;  
	   	}  
	   return result;
	}  


/////////////////////////////////////////////////////////////////////////////////////////////////////////
function MapKeys() {      
	var tmp,tmpEmpty,ChildObjTp,ChildIsAr; 
	var levelCurrent=0; 
	var record=this;
	function isArray(obj) {return typeof(obj)=='object'&&(obj instanceof Array);}  
	//function emptyIf(obj){if (obj=='tojson')   {return  ' ';} else {return obj+' '}; }  //@note date fields return .tojson so strip it
	function emptyIf(obj){if (typeof(this[obj])=='function')   {return  ' ';} else {return obj+' ';} }  //@note date fields return .tojson so strip it
	//var toType = function(obj) { 		// * http://javascriptweblog.wordpress.com/2011/08/08/fixing-the-javascript-typeof-operator/
	//	  return ({}).toString.call(obj).match(/\s([a-zA-Z]+)/)[1].toLowerCase() 
	//	}
	function keysToArray(obj,propArr,levelMax, _level) {
		/**	example: r1=keysToArray(doc,[null,[] ],2,0) 
		 	_level is used for recursion and should always called with 0
		 	if levelMax is negative returns maximum level
		 	levelMax=0 means top level only 2 up to 2nd depth level etc.
		*/ 
			for (var key in obj) { 
				if (obj.hasOwnProperty(key)) { 
					if (obj[key] instanceof Object && ! (obj[key]  instanceof Array))  
						if (levelMax < 0 || _level+1  <= levelMax) { 
							{propArr[1].push(keysToArray(obj[key],[key,[] ],levelMax,_level+1));}
						}
						else {}  //needed coz nested if ?  
					{propArr[1].push(key);} 
				}
			} 
			return  propArr;
		} 
	//----------------------------------------------------------------------------------------------
	function arrayToStr(lst,prevKey,delimiter, inclKeys,levelMax,_level,_levelMaxFound) {
		/**	example: r2=arrayToStr(r1,'','|',true,2,0,0)
		 	_level and _levelMaxFound is used for recursion and should always called with value 0
		 	if levelMax is negative returns maximum level
		 	levelMax=0 means top level only 2 up to 2nd depth level etc.
		*/ 
			var rt,i;
			_levelMaxFound=Math.max(_level,_levelMaxFound);
			if (prevKey !=='') {prevKey += '.';}
			var rtStr ='';  
			if (lst[0])		{prevKey += lst[0]+'.';} 
			if (inclKeys)	{rtStr += prevKey.slice(0,-1);} 
			for (var n in lst[1]) {
				i=lst[1][n];
				if (typeof(i)=='string') {
					rtStr += delimiter + prevKey + i;
				}
				else
				{
					if (levelMax < 0 || _level+1  <= levelMax) {
						rt=arrayToStr(i,prevKey.slice(0,-1),delimiter, inclKeys,levelMax,_level+1,_levelMaxFound);
						rtStr += delimiter + rt[0];
						_levelMaxFound=Math.max(rt[1],_levelMaxFound);
					}
					else {} 
				}
			}
			if (rtStr[0] == delimiter) {rtStr=rtStr.slice(1);}  // Lstrip delimiters if any
			return [rtStr,_levelMaxFound]
		}
	//----------------------------------------------------------------------------------------------
	 
	var keysV=keysToArray(this,[null,[] ] ,parms.levelMax, 0);   // we can't sort here coz array is nested
	keysV = arrayToStr(keysV,'',' ', parms.inclHeaderKeys,-1,0,0); 
	var MaxDepth=keysV[1];
	keysV=keysV[0].split(' ');	// so we can sort 
	keysV.sort();				// sort to make sure indentical records map to same id 
	keysV=keysV.join(' ');
	emit  ({type:'fieldsGrp',fields:keysV}, {cnt:1, percent:0.0,depth:MaxDepth,exampleIds:[this._id]});
}

function ReduceKeys (key, values) { 
    //var total = {cnt:0,percent:0.0,exampleIds:[]}  
    var total = {cnt:0, percent:0.0,depth:values[0].depth,exampleIds:[]}
    for(var i in values) {
    	total.cnt += values[i].cnt; 
    	if (total.exampleIds.length < parms.Reduce_ExamplesMax){ 
    		total.exampleIds = values[i].exampleIds.concat(total.exampleIds);  
    		}
    	} 
    return total;
} 	

function MetaMapKeys() {   
  fields=this._id.fields.split(' ');
  value=this.value;
  var fldname;
  for (var fld in fields) {
	  fldname=fields[fld];
	  emit ({type:'field',fields:fldname}, {cnt:value.cnt, percent:value.percent,depth:(fldname.match(/\./g)||[]).length,exampleIds:value.exampleIds});
  } 
}

function MetaReduceKeys (key, values) { 
    var total = {cnt:0,percent:0.0,depth:values[0].depth,exampleIds:values[0].exampleIds}  
    for(var i in values) {
    	total.cnt += values[i].cnt; 
    	total.percent += values[i].percent; 
    	} 
    return total;
} 
//////////////////////////////////////////////////////////////////////////////////////////  edu tests
function map_closest() {
    var pitt = [-80.064879, 40.612044];
    var phil = [-74.978052, 40.089738];

    function distance(a, b) {
        var dx = a[0] - b[0];
        var dy = a[1] - b[1];
        return Math.sqrt(dx * dx + dy * dy);
    }

    if (distance(this.loc, pitt) < distance(this.loc, phil)) {
        emit("pitt", 1);
    } else {
        emit("phil", 1);
    }
}

function red_closest (key, values) { 
    var total =  0  
    for(var i in values) {
    	total += values[i];
    	print ('val',key,JSON.stringify(values[i])); 
    	 
    	} 
    return total;
}  

/////////////////////////////////////////////////////////////////////////////////////////

//eof//