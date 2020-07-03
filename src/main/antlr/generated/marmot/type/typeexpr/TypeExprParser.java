// Generated from TypeExpr.g4 by ANTLR 4.7.2

package marmot.type.typeexpr;

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class TypeExprParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7.2", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, ID=12, INT=13, STRING=14, LINE_COMMENT=15, COMMENT=16, 
		WS=17;
	public static final int
		RULE_typeIdExpr = 0, RULE_simpleTypeIdExpr = 1, RULE_listTypeIdExpr = 2, 
		RULE_recordTypeIdExpr = 3, RULE_columnTypeIdListExpr = 4, RULE_columnTypeIdExpr = 5, 
		RULE_typeNameExpr = 6, RULE_simpleTypeNameExpr = 7, RULE_listTypeNameExpr = 8, 
		RULE_recordTypeNameExpr = 9, RULE_columnTypeNameListExpr = 10, RULE_columnTypeNameExpr = 11, 
		RULE_sridExpr = 12;
	private static String[] makeRuleNames() {
		return new String[] {
			"typeIdExpr", "simpleTypeIdExpr", "listTypeIdExpr", "recordTypeIdExpr", 
			"columnTypeIdListExpr", "columnTypeIdExpr", "typeNameExpr", "simpleTypeNameExpr", 
			"listTypeNameExpr", "recordTypeNameExpr", "columnTypeNameListExpr", "columnTypeNameExpr", 
			"sridExpr"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'('", "')'", "'['", "']'", "'{'", "'}'", "','", "':'", "'list'", 
			"'record'", "'?'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, null, null, null, 
			"ID", "INT", "STRING", "LINE_COMMENT", "COMMENT", "WS"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "TypeExpr.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public TypeExprParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class TypeIdExprContext extends ParserRuleContext {
		public SimpleTypeIdExprContext simpleTypeIdExpr() {
			return getRuleContext(SimpleTypeIdExprContext.class,0);
		}
		public ListTypeIdExprContext listTypeIdExpr() {
			return getRuleContext(ListTypeIdExprContext.class,0);
		}
		public RecordTypeIdExprContext recordTypeIdExpr() {
			return getRuleContext(RecordTypeIdExprContext.class,0);
		}
		public TypeIdExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_typeIdExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TypeExprVisitor ) return ((TypeExprVisitor<? extends T>)visitor).visitTypeIdExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TypeIdExprContext typeIdExpr() throws RecognitionException {
		TypeIdExprContext _localctx = new TypeIdExprContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_typeIdExpr);
		try {
			setState(29);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INT:
				enterOuterAlt(_localctx, 1);
				{
				setState(26);
				simpleTypeIdExpr();
				}
				break;
			case T__2:
				enterOuterAlt(_localctx, 2);
				{
				setState(27);
				listTypeIdExpr();
				}
				break;
			case T__4:
				enterOuterAlt(_localctx, 3);
				{
				setState(28);
				recordTypeIdExpr();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SimpleTypeIdExprContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(TypeExprParser.INT, 0); }
		public SridExprContext sridExpr() {
			return getRuleContext(SridExprContext.class,0);
		}
		public SimpleTypeIdExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_simpleTypeIdExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TypeExprVisitor ) return ((TypeExprVisitor<? extends T>)visitor).visitSimpleTypeIdExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SimpleTypeIdExprContext simpleTypeIdExpr() throws RecognitionException {
		SimpleTypeIdExprContext _localctx = new SimpleTypeIdExprContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_simpleTypeIdExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(31);
			match(INT);
			setState(36);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__0) {
				{
				setState(32);
				match(T__0);
				setState(33);
				sridExpr();
				setState(34);
				match(T__1);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ListTypeIdExprContext extends ParserRuleContext {
		public TypeIdExprContext typeIdExpr() {
			return getRuleContext(TypeIdExprContext.class,0);
		}
		public ListTypeIdExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listTypeIdExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TypeExprVisitor ) return ((TypeExprVisitor<? extends T>)visitor).visitListTypeIdExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ListTypeIdExprContext listTypeIdExpr() throws RecognitionException {
		ListTypeIdExprContext _localctx = new ListTypeIdExprContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_listTypeIdExpr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(38);
			match(T__2);
			setState(39);
			typeIdExpr();
			setState(40);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RecordTypeIdExprContext extends ParserRuleContext {
		public ColumnTypeIdListExprContext columnTypeIdListExpr() {
			return getRuleContext(ColumnTypeIdListExprContext.class,0);
		}
		public RecordTypeIdExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_recordTypeIdExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TypeExprVisitor ) return ((TypeExprVisitor<? extends T>)visitor).visitRecordTypeIdExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RecordTypeIdExprContext recordTypeIdExpr() throws RecognitionException {
		RecordTypeIdExprContext _localctx = new RecordTypeIdExprContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_recordTypeIdExpr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(42);
			match(T__4);
			setState(43);
			columnTypeIdListExpr();
			setState(44);
			match(T__5);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnTypeIdListExprContext extends ParserRuleContext {
		public List<ColumnTypeIdExprContext> columnTypeIdExpr() {
			return getRuleContexts(ColumnTypeIdExprContext.class);
		}
		public ColumnTypeIdExprContext columnTypeIdExpr(int i) {
			return getRuleContext(ColumnTypeIdExprContext.class,i);
		}
		public ColumnTypeIdListExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnTypeIdListExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TypeExprVisitor ) return ((TypeExprVisitor<? extends T>)visitor).visitColumnTypeIdListExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnTypeIdListExprContext columnTypeIdListExpr() throws RecognitionException {
		ColumnTypeIdListExprContext _localctx = new ColumnTypeIdListExprContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_columnTypeIdListExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(46);
			columnTypeIdExpr();
			setState(51);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__6) {
				{
				{
				setState(47);
				match(T__6);
				setState(48);
				columnTypeIdExpr();
				}
				}
				setState(53);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnTypeIdExprContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(TypeExprParser.ID, 0); }
		public TypeIdExprContext typeIdExpr() {
			return getRuleContext(TypeIdExprContext.class,0);
		}
		public ColumnTypeIdExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnTypeIdExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TypeExprVisitor ) return ((TypeExprVisitor<? extends T>)visitor).visitColumnTypeIdExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnTypeIdExprContext columnTypeIdExpr() throws RecognitionException {
		ColumnTypeIdExprContext _localctx = new ColumnTypeIdExprContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_columnTypeIdExpr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(54);
			match(ID);
			setState(55);
			match(T__7);
			setState(56);
			typeIdExpr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TypeNameExprContext extends ParserRuleContext {
		public SimpleTypeNameExprContext simpleTypeNameExpr() {
			return getRuleContext(SimpleTypeNameExprContext.class,0);
		}
		public ListTypeNameExprContext listTypeNameExpr() {
			return getRuleContext(ListTypeNameExprContext.class,0);
		}
		public RecordTypeNameExprContext recordTypeNameExpr() {
			return getRuleContext(RecordTypeNameExprContext.class,0);
		}
		public TypeNameExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_typeNameExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TypeExprVisitor ) return ((TypeExprVisitor<? extends T>)visitor).visitTypeNameExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TypeNameExprContext typeNameExpr() throws RecognitionException {
		TypeNameExprContext _localctx = new TypeNameExprContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_typeNameExpr);
		try {
			setState(61);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(58);
				simpleTypeNameExpr();
				}
				break;
			case T__8:
				enterOuterAlt(_localctx, 2);
				{
				setState(59);
				listTypeNameExpr();
				}
				break;
			case T__9:
				enterOuterAlt(_localctx, 3);
				{
				setState(60);
				recordTypeNameExpr();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SimpleTypeNameExprContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(TypeExprParser.ID, 0); }
		public SridExprContext sridExpr() {
			return getRuleContext(SridExprContext.class,0);
		}
		public SimpleTypeNameExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_simpleTypeNameExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TypeExprVisitor ) return ((TypeExprVisitor<? extends T>)visitor).visitSimpleTypeNameExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SimpleTypeNameExprContext simpleTypeNameExpr() throws RecognitionException {
		SimpleTypeNameExprContext _localctx = new SimpleTypeNameExprContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_simpleTypeNameExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(63);
			match(ID);
			setState(68);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__0) {
				{
				setState(64);
				match(T__0);
				setState(65);
				sridExpr();
				setState(66);
				match(T__1);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ListTypeNameExprContext extends ParserRuleContext {
		public TypeNameExprContext typeNameExpr() {
			return getRuleContext(TypeNameExprContext.class,0);
		}
		public ListTypeNameExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listTypeNameExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TypeExprVisitor ) return ((TypeExprVisitor<? extends T>)visitor).visitListTypeNameExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ListTypeNameExprContext listTypeNameExpr() throws RecognitionException {
		ListTypeNameExprContext _localctx = new ListTypeNameExprContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_listTypeNameExpr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(70);
			match(T__8);
			setState(71);
			match(T__2);
			setState(72);
			typeNameExpr();
			setState(73);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RecordTypeNameExprContext extends ParserRuleContext {
		public ColumnTypeNameListExprContext columnTypeNameListExpr() {
			return getRuleContext(ColumnTypeNameListExprContext.class,0);
		}
		public RecordTypeNameExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_recordTypeNameExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TypeExprVisitor ) return ((TypeExprVisitor<? extends T>)visitor).visitRecordTypeNameExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RecordTypeNameExprContext recordTypeNameExpr() throws RecognitionException {
		RecordTypeNameExprContext _localctx = new RecordTypeNameExprContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_recordTypeNameExpr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(75);
			match(T__9);
			setState(76);
			match(T__4);
			setState(77);
			columnTypeNameListExpr();
			setState(78);
			match(T__5);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnTypeNameListExprContext extends ParserRuleContext {
		public List<ColumnTypeNameExprContext> columnTypeNameExpr() {
			return getRuleContexts(ColumnTypeNameExprContext.class);
		}
		public ColumnTypeNameExprContext columnTypeNameExpr(int i) {
			return getRuleContext(ColumnTypeNameExprContext.class,i);
		}
		public ColumnTypeNameListExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnTypeNameListExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TypeExprVisitor ) return ((TypeExprVisitor<? extends T>)visitor).visitColumnTypeNameListExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnTypeNameListExprContext columnTypeNameListExpr() throws RecognitionException {
		ColumnTypeNameListExprContext _localctx = new ColumnTypeNameListExprContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_columnTypeNameListExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(80);
			columnTypeNameExpr();
			setState(85);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__6) {
				{
				{
				setState(81);
				match(T__6);
				setState(82);
				columnTypeNameExpr();
				}
				}
				setState(87);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnTypeNameExprContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(TypeExprParser.ID, 0); }
		public TypeNameExprContext typeNameExpr() {
			return getRuleContext(TypeNameExprContext.class,0);
		}
		public ColumnTypeNameExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnTypeNameExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TypeExprVisitor ) return ((TypeExprVisitor<? extends T>)visitor).visitColumnTypeNameExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnTypeNameExprContext columnTypeNameExpr() throws RecognitionException {
		ColumnTypeNameExprContext _localctx = new ColumnTypeNameExprContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_columnTypeNameExpr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(88);
			match(ID);
			setState(89);
			match(T__7);
			setState(90);
			typeNameExpr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SridExprContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(TypeExprParser.INT, 0); }
		public SridExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sridExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof TypeExprVisitor ) return ((TypeExprVisitor<? extends T>)visitor).visitSridExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SridExprContext sridExpr() throws RecognitionException {
		SridExprContext _localctx = new SridExprContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_sridExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(92);
			_la = _input.LA(1);
			if ( !(_la==T__10 || _la==INT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\23a\4\2\t\2\4\3\t"+
		"\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t\13\4"+
		"\f\t\f\4\r\t\r\4\16\t\16\3\2\3\2\3\2\5\2 \n\2\3\3\3\3\3\3\3\3\3\3\5\3"+
		"\'\n\3\3\4\3\4\3\4\3\4\3\5\3\5\3\5\3\5\3\6\3\6\3\6\7\6\64\n\6\f\6\16\6"+
		"\67\13\6\3\7\3\7\3\7\3\7\3\b\3\b\3\b\5\b@\n\b\3\t\3\t\3\t\3\t\3\t\5\t"+
		"G\n\t\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f\7\fV\n"+
		"\f\f\f\16\fY\13\f\3\r\3\r\3\r\3\r\3\16\3\16\3\16\2\2\17\2\4\6\b\n\f\16"+
		"\20\22\24\26\30\32\2\3\4\2\r\r\17\17\2[\2\37\3\2\2\2\4!\3\2\2\2\6(\3\2"+
		"\2\2\b,\3\2\2\2\n\60\3\2\2\2\f8\3\2\2\2\16?\3\2\2\2\20A\3\2\2\2\22H\3"+
		"\2\2\2\24M\3\2\2\2\26R\3\2\2\2\30Z\3\2\2\2\32^\3\2\2\2\34 \5\4\3\2\35"+
		" \5\6\4\2\36 \5\b\5\2\37\34\3\2\2\2\37\35\3\2\2\2\37\36\3\2\2\2 \3\3\2"+
		"\2\2!&\7\17\2\2\"#\7\3\2\2#$\5\32\16\2$%\7\4\2\2%\'\3\2\2\2&\"\3\2\2\2"+
		"&\'\3\2\2\2\'\5\3\2\2\2()\7\5\2\2)*\5\2\2\2*+\7\6\2\2+\7\3\2\2\2,-\7\7"+
		"\2\2-.\5\n\6\2./\7\b\2\2/\t\3\2\2\2\60\65\5\f\7\2\61\62\7\t\2\2\62\64"+
		"\5\f\7\2\63\61\3\2\2\2\64\67\3\2\2\2\65\63\3\2\2\2\65\66\3\2\2\2\66\13"+
		"\3\2\2\2\67\65\3\2\2\289\7\16\2\29:\7\n\2\2:;\5\2\2\2;\r\3\2\2\2<@\5\20"+
		"\t\2=@\5\22\n\2>@\5\24\13\2?<\3\2\2\2?=\3\2\2\2?>\3\2\2\2@\17\3\2\2\2"+
		"AF\7\16\2\2BC\7\3\2\2CD\5\32\16\2DE\7\4\2\2EG\3\2\2\2FB\3\2\2\2FG\3\2"+
		"\2\2G\21\3\2\2\2HI\7\13\2\2IJ\7\5\2\2JK\5\16\b\2KL\7\6\2\2L\23\3\2\2\2"+
		"MN\7\f\2\2NO\7\7\2\2OP\5\26\f\2PQ\7\b\2\2Q\25\3\2\2\2RW\5\30\r\2ST\7\t"+
		"\2\2TV\5\30\r\2US\3\2\2\2VY\3\2\2\2WU\3\2\2\2WX\3\2\2\2X\27\3\2\2\2YW"+
		"\3\2\2\2Z[\7\16\2\2[\\\7\n\2\2\\]\5\16\b\2]\31\3\2\2\2^_\t\2\2\2_\33\3"+
		"\2\2\2\b\37&\65?FW";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}