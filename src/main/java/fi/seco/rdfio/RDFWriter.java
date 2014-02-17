package fi.seco.rdfio;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.apache.jena.riot.RIOT;
import org.apache.lucene.util.OpenBitSet;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.procedures.LongObjectProcedure;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import fi.seco.collections.map.primitive.AEnsuredLongObjectHashMap;
import fi.seco.collections.map.primitive.EnsuredLongObjectHashMap;
import fi.seco.collections.map.primitive.IEnsuredLongObjectMap;
import fi.seco.rdfio.RDFReader.IRDFHandler;
import fi.seco.rdfobject.IQuad;
import fi.seco.rdfobject.IRDFObject;
import fi.seco.rdfobject.ITriple;
import fi.seco.rdfobject.Quad;
import fi.seco.rdfobject.URIResourceRDFObject;
import fi.seco.rdfobject.jena.JenaRDFObjectUtil;
import fi.seco.rdfobject.model.IRDFObjectQuadModel;
import fi.seco.rdfobject.model.IRDFObjectTripleModel;
import fi.seco.rdfobject.openrdf.OpenRDFRDFObjectUtil;

/**
 * A utility class for writing RDF. Uses efficient streaming writers when
 * possible. Can write at least turtle, n3, RDF/XML, trig, trix, n-triples and
 * n-quads.
 * 
 * @author jiemakel
 * 
 */
public class RDFWriter {

	private static final Logger log = LoggerFactory.getLogger(RDFWriter.class);

	static {
		RDFWriterRegistry.getInstance(); //.add(new RDFXMLPrettyWriterFactory());// for initialization of formats;
		RIOT.init();
	}

	/**
	 * An RDF writer
	 * 
	 * @author jiemakel
	 * 
	 */
	public static interface IRDFWriter extends IRDFHandler {
		/**
		 * Used to tell the writer all metadata information (prefixes) has been
		 * passed in. Must be called before feeding quads.
		 */
		public void endProlog();

		/**
		 * Used to tell the writer all quads have been passed in, allowing the
		 * writer to close the output stream.
		 */
		public void close();
	}

	private static void recursivelyProcessSubject(long g, long s, LongObjectMap<LongArrayList> triples,
			LongObjectMap<IRDFObject> idObjectMap, OpenBitSet oc2, org.openrdf.rio.RDFWriter w) throws RDFHandlerException {
		IRDFObject so = idObjectMap.get(s);
		LongArrayList tmp = triples.get(s);
		Iterator<LongCursor> tli = tmp.iterator();
		for (int k = tmp.size(); k > 0; k -= 3) {
			long p = tli.next().value;
			long o = tli.next().value;
			w.handleStatement(OpenRDFRDFObjectUtil.getStatementForQuad(so, idObjectMap.get(p), idObjectMap.get(o), idObjectMap.get(g)));
			if (!oc2.get(o) && triples.containsKey(o)) recursivelyProcessSubject(g, o, triples, idObjectMap, oc2, w);
		}
	}

	private static final long getId(IRDFObject oo, long[] lid, LongObjectMap<IRDFObject> idObjectMap,
			ObjectLongMap<IRDFObject> objectIdMap) {
		long o = objectIdMap.get(oo);
		if (o == 0) {
			o = lid[0]++;
			objectIdMap.put(oo, o);
			idObjectMap.put(o, oo);
		}
		return o;
	}

	public static void write(IRDFObjectQuadModel m, IRDFWriter w) {
		for (Map.Entry<String, String> e : m.getPrefixMap().entrySet())
			w.setNameSpace(e.getKey(), e.getValue());
		w.endProlog();
		for (IQuad q : m.listQuads())
			w.visit(q);
		w.close();
	}

	private static final IRDFObject dg = new URIResourceRDFObject("urn:x-arq:DefaultGraph");

	public static void write(IRDFObjectTripleModel m, IRDFWriter w) {
		for (Map.Entry<String, String> e : m.getPrefixMap().entrySet())
			w.setNameSpace(e.getKey(), e.getValue());
		w.endProlog();
		for (ITriple q : m.listTriples())
			w.visit(new Quad(q, dg));
		w.close();
	}

	/**
	 * Returns an RDF writer that can be used to write out triples or quads
	 * 
	 * @param output
	 *            an OutputStream to write to
	 * @param type
	 *            the format to write
	 * @param pretty
	 *            a request a pretty writer, if it makes sense for the format.
	 *            This usually turns off streaming. Pretty writers are available
	 *            for turtle, n3, RDF/XML, trig and trix.
	 * @return an RDF writer for writing the specified format
	 */
	public static IRDFWriter getWriter(final OutputStream output, final RDFFormat type, boolean pretty) {
		if (pretty) if (RDFFormat.N3.equals(type) || RDFFormat.TURTLE.equals(type) || RDFFormat.RDFXML.equals(type)) {
			//Requested N3 or TURTLE & pretty writer, Jena does it best
			final Model m = ModelFactory.createDefaultModel();
			final String format;
			if (RDFFormat.N3.equals(type))
				format = "N3";
			else if (RDFFormat.TURTLE.equals(type))
				format = "TURTLE";
			else format = "RDF/XML-ABBREV";
			return new IRDFWriter() {

				@Override
				public void setNameSpace(String prefix, String ns) {
					m.setNsPrefix(prefix, ns);
				}

				@Override
				public void setBaseIRI(String baseIRI) {
					if (baseIRI != null) m.setNsPrefix("", baseIRI);
				}

				@Override
				public void comment(String comment) {}

				@Override
				public void visit(IQuad q) {
					m.add(JenaRDFObjectUtil.getJenaStatementForTriple(q));
				}

				@Override
				public void endProlog() {}

				@Override
				public void close() {
					m.write(output, format);
					try {
						output.close();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}

			};
		} /*else if (RDFFormat.RDFXML.equals(type)) { // by st,with oc
			final org.openrdf.rio.RDFWriter w = Rio.createWriter(type, output);
			final LongObjectMap<IRDFObject> idObjectMap = new LongObjectOpenHashMap<IRDFObject>();
			final TObjectLongMap<IRDFObject> objectIdMap = new TObjectLongHashMap<IRDFObject>();
			final IEnsuredLongObjectMap<LongArrayList> stMap = new EnsuredLongObjectHashMap<LongArrayList>(LongArrayList.class);
			final long[] lid = new long[] { 1 };
			final OpenBitSet oc1 = new OpenBitSet();
			final OpenBitSet oc2 = new OpenBitSet();
			return new IRDFWriter() {

				@Override
				public void setNameSpace(String prefix, String ns) {
					try {
						w.handleNamespace(prefix, ns);
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void comment(String comment) {
					try {
						w.handleComment(comment);
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void visit(IQuad q) {
					long s = getId(q.getSubject(), lid, idObjectMap, objectIdMap);
					long p = getId(q.getProperty(), lid, idObjectMap, objectIdMap);
					long o = getId(q.getObject(), lid, idObjectMap, objectIdMap);
					if (oc1.get(o))
						oc2.set(o);
					else oc1.set(o);
					LongArrayList tmp = stMap.ensure(s);
					tmp.add(p);
					tmp.add(o);
				}

				@Override
				public void endProlog() {
					try {
						w.startRDF();
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void close() {
					try {
						for (long s : stMap.keys())
							if (oc2.get(s) || !oc1.get(s)) recursivelyProcessSubject(0, s, stMap, idObjectMap, oc2, w);
						w.endRDF();
						output.close();
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}

			};
			}*/else if (RDFFormat.TRIG.equals(type)) { // by gst
			final org.openrdf.rio.RDFWriter w = Rio.createWriter(type, output);
			final LongObjectMap<IRDFObject> idObjectMap = new LongObjectOpenHashMap<IRDFObject>();
			final ObjectLongMap<IRDFObject> objectIdMap = new ObjectLongOpenHashMap<IRDFObject>();
			final IEnsuredLongObjectMap<IEnsuredLongObjectMap<LongArrayList>> gstMap = new AEnsuredLongObjectHashMap<IEnsuredLongObjectMap<LongArrayList>>() {

				@Override
				protected IEnsuredLongObjectMap<LongArrayList> createNew(long g) {
					return new EnsuredLongObjectHashMap<LongArrayList>(LongArrayList.class);
				}

			};
			final long[] lid = new long[] { 1 };
			return new IRDFWriter() {

				@Override
				public void setNameSpace(String prefix, String ns) {
					try {
						w.handleNamespace(prefix, ns);
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void setBaseIRI(String baseIRI) {
					if (baseIRI != null) try {
						w.handleNamespace("", baseIRI);
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void comment(String comment) {
					try {
						w.handleComment(comment);
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void visit(IQuad q) {
					long s = getId(q.getSubject(), lid, idObjectMap, objectIdMap);
					long p = getId(q.getProperty(), lid, idObjectMap, objectIdMap);
					long o = getId(q.getObject(), lid, idObjectMap, objectIdMap);
					long g = getId(q.getGraph(), lid, idObjectMap, objectIdMap);
					LongArrayList tmp = gstMap.ensure(g).ensure(s);
					tmp.add(p);
					tmp.add(o);
				}

				@Override
				public void endProlog() {
					try {
						w.startRDF();
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void close() {
					try {
						for (final LongCursor g : gstMap.keys())
							gstMap.get(g.value).forEach(new LongObjectProcedure<LongArrayList>() {

								@Override
								public void apply(long s, LongArrayList st2) {
									IRDFObject so = idObjectMap.get(s);
									Iterator<LongCursor> sti2 = st2.iterator();
									for (int j = st2.size(); j > 0; j -= 2) {
										long p = sti2.next().value;
										long o = sti2.next().value;
										try {
											w.handleStatement(OpenRDFRDFObjectUtil.getStatementForQuad(so, idObjectMap.get(p), idObjectMap.get(o), idObjectMap.get(g.value)));
										} catch (RDFHandlerException e) {
											throw new RuntimeException(e);
										}
									}
								}

							});
						w.endRDF();
						output.close();
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}

			};
		} else if (RDFFormat.TRIX.equals(type)) { // by g
			final org.openrdf.rio.RDFWriter w = Rio.createWriter(type, output);
			final LongObjectMap<IRDFObject> idObjectMap = new LongObjectOpenHashMap<IRDFObject>();
			final ObjectLongMap<IRDFObject> objectIdMap = new ObjectLongOpenHashMap<IRDFObject>();
			final IEnsuredLongObjectMap<LongArrayList> gtMap = new EnsuredLongObjectHashMap<LongArrayList>(LongArrayList.class);
			final long[] lid = new long[] { 1 };
			return new IRDFWriter() {

				@Override
				public void setNameSpace(String prefix, String ns) {
					try {
						w.handleNamespace(prefix, ns);
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void setBaseIRI(String baseIRI) {
					if (baseIRI != null) try {
						w.handleNamespace("", baseIRI);
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void comment(String comment) {
					try {
						w.handleComment(comment);
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void visit(IQuad q) {
					long s = getId(q.getSubject(), lid, idObjectMap, objectIdMap);
					long p = getId(q.getProperty(), lid, idObjectMap, objectIdMap);
					long o = getId(q.getObject(), lid, idObjectMap, objectIdMap);
					long g = getId(q.getGraph(), lid, idObjectMap, objectIdMap);
					LongArrayList tmp = gtMap.ensure(g);
					tmp.add(s);
					tmp.add(p);
					tmp.add(o);
				}

				@Override
				public void endProlog() {
					try {
						w.startRDF();
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void close() {
					try {
						gtMap.forEach(new LongObjectProcedure<LongArrayList>() {

							@Override
							public void apply(long g, LongArrayList st2) {
								IRDFObject go = idObjectMap.get(g);
								Iterator<LongCursor> sti2 = st2.iterator();
								for (int j = st2.size(); j > 0; j -= 3) {
									long s = sti2.next().value;
									long p = sti2.next().value;
									long o = sti2.next().value;
									try {
										w.handleStatement(OpenRDFRDFObjectUtil.getStatementForQuad(idObjectMap.get(s), idObjectMap.get(p), idObjectMap.get(o), go));
									} catch (RDFHandlerException e) {
										throw new RuntimeException(e);
									}
								}
							}
						});
						w.endRDF();
						output.close();
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}

			};
		}
		if (type != null) {
			final org.openrdf.rio.RDFWriter w = Rio.createWriter(type, output);
			return new IRDFWriter() {

				@Override
				public void setNameSpace(String prefix, String ns) {
					try {
						w.handleNamespace(prefix, ns);
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void setBaseIRI(String baseIRI) {
					if (baseIRI != null) try {
						w.handleNamespace("", baseIRI);
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void comment(String comment) {
					try {
						w.handleComment(comment);
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void visit(IQuad q) {
					try {
						w.handleStatement(OpenRDFRDFObjectUtil.getStatementForQuad(q));
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void endProlog() {
					try {
						w.startRDF();
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void close() {
					try {

						w.endRDF();
						output.close();
					} catch (RDFHandlerException e) {
						throw new RuntimeException(e);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}

			};
		}
		return null;
	}

	public static IRDFWriter getWriter(final String filename, boolean pretty) {
		RDFFormat type = RDFReader.getFormat(filename);
		OutputStream fo;
		try {
			fo = new FileOutputStream(filename);
			if (filename.endsWith(".gz"))
				fo = new GZIPOutputStream(fo);
			else if (filename.endsWith(".bz2"))
				fo = new BZip2CompressorOutputStream(fo);
			else if (filename.endsWith(".xz")) fo = new XZCompressorOutputStream(fo);
		} catch (IOException e) {
			log.error("Couldn't write model to file " + filename, e);
			return null;
		}
		return getWriter(fo, type, pretty);
	}
}
