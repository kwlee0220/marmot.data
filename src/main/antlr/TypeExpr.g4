grammar TypeExpr;

@header {
package marmot.type.typeexpr;
}

typeIdExpr			: simpleTypeIdExpr | listTypeIdExpr | recordTypeIdExpr;
simpleTypeIdExpr	: INT ( '(' sridExpr ')' )?;
listTypeIdExpr		: '[' typeIdExpr ']';
recordTypeIdExpr	: '{' columnTypeIdListExpr '}';
columnTypeIdListExpr: columnTypeIdExpr ( ',' columnTypeIdExpr )*;
columnTypeIdExpr	: ID ':' typeIdExpr;


typeNameExpr		: simpleTypeNameExpr | listTypeNameExpr | recordTypeNameExpr;
simpleTypeNameExpr	: ID ( '(' sridExpr ')' )?;
listTypeNameExpr	: 'list' '[' typeNameExpr ']';
recordTypeNameExpr	: 'record' '{' columnTypeNameListExpr '}';
columnTypeNameListExpr	: columnTypeNameExpr ( ',' columnTypeNameExpr )*;
columnTypeNameExpr	: ID ':' typeNameExpr;

sridExpr			: ('?' | INT);
	
ID		:	ID_LETTER (ID_LETTER | DIGIT)* ;
INT 	:	DIGIT+;

fragment ID_LETTER :	'a'..'z'|'A'..'Z'|'_'|'/'|'$'| '\u0080'..'\ufffe' ;
fragment DIGIT :	'0'..'9' ;

STRING	:	'\'' (ESC|.)*? '\'' ;
fragment ESC :	'\\' [btnr"\\] ;

LINE_COMMENT:	'//' .*? '\n' -> skip ;
COMMENT		:	'/*' .*? '*/' -> skip ;

WS	:	[ \t\r\n]+ -> skip;
