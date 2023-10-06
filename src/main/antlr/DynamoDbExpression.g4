grammar DynamoDbExpression;

@header {
   package com.cjuega.easydynamodb.expressions;
}

condition        : orCondition;
orCondition      : andCondition (OR andCondition)*;
andCondition     : primaryCondition (AND primaryCondition)*;
primaryCondition :
    attributeName COMPARATOR attributeValue
    | attributeName IN '(' attributeValueList ')'
    | attributeName BETWEEN attributeValue AND attributeValue
    | function
    | '(' condition ')'
    | NOT primaryCondition
    ;

function :
    FUNCTION_ATTRIBUTE_EXISTS '(' attributePath ')'
    | FUNCTION_ATTRIBUTE_NOT_EXISTS '(' attributePath ')'
    | FUNCTION_ATTRIBUTE_TYPE '(' attributePath ',' TYPE ')'
    | FUNCTION_BEGINS_WITH '(' attributePath ',' attributeValue ')'
    | FUNCTION_CONTAINS '(' attributePath ',' attributeValue ')'
    | FUNCTION_SIZE '(' attributePath ')'
    | FUNCTION_SIZE '(' attributePath ')' COMPARATOR attributeValue
    | attributeValue COMPARATOR FUNCTION_SIZE '(' attributePath ')'
    ;

attributeName : ID;
attributePath : attributeName ( '.' attributeName )*;
attributeValue : NUMBER | STRING | BOOLEAN;
attributeValueList : attributeValue ( ',' attributeValue )*;

COMPARATOR : '=' | '<>' | '<' | '<=' | '>' | '>=';
AND        : 'AND' | 'and';
OR         : 'OR' | 'or';
NOT        : 'NOT' | 'not';
BETWEEN    : 'BETWEEN' | 'between';
IN         : 'IN' | 'in';

FUNCTION_ATTRIBUTE_EXISTS     : 'attribute_exists';
FUNCTION_ATTRIBUTE_NOT_EXISTS : 'attribute_not_exists';
FUNCTION_ATTRIBUTE_TYPE       : 'attribute_type';
FUNCTION_BEGINS_WITH          : 'begins_with';
FUNCTION_CONTAINS             : 'contains';
FUNCTION_SIZE                 : 'size';

TYPE : 'S' | 'SS' | 'N' | 'NS' | 'B' | 'BS' | 'BOOL' | 'NULL' | 'L' | 'M';

BOOLEAN     : 'true' | 'false' ;
NUMBER      : [0-9]+ ('.' [0-9]+)?;
STRING      : '"' .*? '"' ;
//REPLACEMENT : '@{'~'}'*'}';
ID          : [a-zA-Z_][a-zA-Z0-9_]*;

WS : [ \t\r\n]+ -> skip;
