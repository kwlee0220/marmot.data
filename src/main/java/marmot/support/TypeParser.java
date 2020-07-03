package marmot.support;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import marmot.Column;
import marmot.RecordSchema;
import marmot.type.DataType;
import marmot.type.GeometryDataType;
import marmot.type.PrimitiveDataType;
import marmot.type.RecordType;
import marmot.type.typeexpr.TypeExprBaseVisitor;
import marmot.type.typeexpr.TypeExprLexer;
import marmot.type.typeexpr.TypeExprParser;
import marmot.type.typeexpr.TypeExprParser.ColumnTypeIdExprContext;
import marmot.type.typeexpr.TypeExprParser.ColumnTypeNameExprContext;
import marmot.type.typeexpr.TypeExprParser.SridExprContext;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TypeParser {
	public static final void main(String... args) throws Exception {
		DataType type;
		type = TypeParser.parseTypeId("{'id':3, name:8, ratio:6}");
		System.out.println(type);
	}
	
	public static DataType parseTypeId(String typeIdExpr) {
		typeIdExpr = typeIdExpr.trim();
		if ( typeIdExpr.length() == 0 ) {
			return DataType.NULL;
		}
		
		TypeExprLexer lexer = new TypeExprLexer(new ANTLRInputStream(typeIdExpr));
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		TypeExprParser parser = new TypeExprParser(tokens);
		
		ParseTree tree = parser.typeIdExpr();
		Visitor visitor = new Visitor();
		return (DataType)visitor.visit(tree);
	}
	
	public static DataType parseTypeName(String typeNameExpr) {
		typeNameExpr = typeNameExpr.trim();
		if ( typeNameExpr.length() == 0 ) {
			return DataType.NULL;
		}
		
		TypeExprLexer lexer = new TypeExprLexer(new ANTLRInputStream(typeNameExpr));
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		TypeExprParser parser = new TypeExprParser(tokens);
		
		ParseTree tree = parser.typeNameExpr();
		Visitor visitor = new Visitor();
		return (DataType)visitor.visit(tree);
	}
	
	public static RecordSchema parseRecordSchemaFromTypeId(String schemaId) {
		schemaId = schemaId.trim();
		if ( schemaId.length() == 0 ) {
			return RecordSchema.NULL;
		}
		
		TypeExprLexer lexer = new TypeExprLexer(new ANTLRInputStream(schemaId));
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		TypeExprParser parser = new TypeExprParser(tokens);
		
		ParseTree tree = parser.recordTypeIdExpr();
		Visitor visitor = new Visitor();
		return ((RecordType)visitor.visit(tree)).getRecordSchema();
	}
	
	public static RecordSchema parseRecordSchemaFromTypeName(String typeName) {
		typeName = typeName.trim();
		if ( typeName.length() == 0 ) {
			return RecordSchema.NULL;
		}
		
		TypeExprLexer lexer = new TypeExprLexer(new ANTLRInputStream(typeName));
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		TypeExprParser parser = new TypeExprParser(tokens);
		
		ParseTree tree = parser.recordTypeNameExpr();
		Visitor visitor = new Visitor();
		return ((RecordType)visitor.visit(tree)).getRecordSchema();
	}
	
	static class Visitor extends TypeExprBaseVisitor<Object> {
		// typeIdExpr			: simpleTypeIdExpr | listTypeIdExpr | recordTypeIdExpr;
		@Override
		public Object visitTypeIdExpr(TypeExprParser.TypeIdExprContext ctx) {
			return visit(ctx.getChild(0));
		}
		
		// simpleTypeIdExpr	: INT ( '(' sridExpr ')' )?;
		@Override
		public Object visitSimpleTypeIdExpr(TypeExprParser.SimpleTypeIdExprContext ctx) {
			String tcStr = ctx.getChild(0).getText();
			DataType type = PrimitiveDataType.fromTypeCode(Integer.parseInt(tcStr));
			if ( type == null ) {
				throw new IllegalArgumentException("unknown type code: " + tcStr);
			}
			
			ParseTree suffixPT = ctx.getChild(2);
			if ( suffixPT == null ) {
				return type;
			}
			String srid = (String)visit(suffixPT);
			if ( !srid.equals("*") ) {
				srid = "EPSG:" + srid;
			}
			return ((GeometryDataType)type).duplicate(srid);
		}
		
		// listTypeIdExpr		: '[' typeIdExpr ']';
		@Override
		public Object visitListTypeIdExpr(TypeExprParser.ListTypeIdExprContext ctx) {
			DataType elmType = (DataType)visit(ctx.getChild(1));
			return DataType.LIST(elmType);
		}

		// recordTypeIdExpr	: '{' columnTypeIdListExpr '}';
		@Override
		public Object visitRecordTypeIdExpr(TypeExprParser.RecordTypeIdExprContext ctx) {
			RecordSchema schema = (RecordSchema)visit(ctx.getChild(1));
			return DataType.RECORD(schema);
		}
		
		// columnTypeIdListExpr	: columnTypeIdExpr ( ',' columnTypeIdExpr )*;
		@Override
		public Object visitColumnTypeIdListExpr(TypeExprParser.ColumnTypeIdListExprContext ctx) {
			return FStream.from(ctx.children)
							.castSafely(ColumnTypeIdExprContext.class)
							.map(this::visit)
							.cast(Column.class)
							.foldLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
							.build();
		}

		// columnTypeIdExpr	: ID ':' typeIdExpr;
		@Override
		public Object visitColumnTypeIdExpr(TypeExprParser.ColumnTypeIdExprContext ctx) {
			String colName = ctx.getChild(0).getText();
			DataType colType = (DataType)visit(ctx.getChild(2));
			return new Column(colName, colType);
		}
		
		// typeNameExpr		: simpleTypeNameExpr | listTypeNameExpr | recordTypeNameExpr;
		@Override
		public Object visitTypeNameExpr(TypeExprParser.TypeNameExprContext ctx) {
			return visit(ctx.getChild(0));
		}
		
		// simpleTypeNameExpr	: ID ( '(' sridExpr ')' )?;
		@Override
		public Object visitSimpleTypeNameExpr(TypeExprParser.SimpleTypeNameExprContext ctx) {
			String tcName = ctx.getChild(0).getText();
			DataType type = PrimitiveDataType.fromTypeCodeName(tcName);
			if ( type == null ) {
				throw new IllegalArgumentException("unknown type code: " + tcName);
			}
			
			ParseTree suffixPT = ctx.getChild(2);
			if ( suffixPT == null ) {
				return type;
			}
			String srid = (String)visit(suffixPT);
			if ( !srid.equals("?") ) {
				srid = "EPSG:" + srid;
				return ((GeometryDataType)type).duplicate(srid);
			}
			else {
				return type;
			}
		}
		
		// listTypeNameExpr	: 'list' '[' typeNameExpr ']';
		@Override
		public Object visitListTypeNameExpr(TypeExprParser.ListTypeNameExprContext ctx) {
			DataType elmType = (DataType)visit(ctx.getChild(2));
			return DataType.LIST(elmType);
		}

		// recordTypeNameExpr	: 'record' '{' columnTypeNameListExpr '}';
		@Override
		public Object visitRecordTypeNameExpr(TypeExprParser.RecordTypeNameExprContext ctx) {
			RecordSchema schema = (RecordSchema)visit(ctx.getChild(2));
			return DataType.RECORD(schema);
		}
		
		// columnTypeNameListExpr	: columnTypeNameExpr ( ',' columnTypeNameExpr )*;
		@Override
		public Object visitColumnTypeNameListExpr(TypeExprParser.ColumnTypeNameListExprContext ctx) {
			return FStream.from(ctx.children)
							.castSafely(ColumnTypeNameExprContext.class)
							.map(this::visit)
							.cast(Column.class)
							.foldLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
							.build();
		}

		// columnTypeNameExpr	: ID ':' typeNameExpr;
		@Override
		public Object visitColumnTypeNameExpr(TypeExprParser.ColumnTypeNameExprContext ctx) {
			String colName = ctx.getChild(0).getText();
			DataType colType = (DataType)visit(ctx.getChild(2));
			return new Column(colName, colType);
		}

		// sridExpr		: ('*' | INT);
		@Override
		public Object visitSridExpr(SridExprContext ctx) {
			return ctx.getChild(0).getText();
		}
	}
}
