package it.uniroma2.taos.commons.persistence;

import it.uniroma2.taos.commons.Constants;

import java.util.concurrent.TimeUnit;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;

public class Database {

    // node cpu utilizazion in Mhz
    private MongoCollection<Document> nc;
    // executor cpu utilization in Mhz
    private MongoCollection<Document> ec;
    // traffic between executors
    private MongoCollection<Document> tr;
    // current topology
    private MongoCollection<Document> to;
    // numero di executor con timestamp
    private MongoCollection<Document> ne;
    // tempo di migrazione con tempo e timestamp
    private MongoCollection<Document> mt;
    // tempo di risposta medio
    private MongoCollection<Document> rt;
    // size delle migrazioni parsiale
    private MongoCollection<Document> tsm;
    // migration size totale
    private MongoCollection<Document> sm;
    // Network cost
    private MongoCollection<Document> nt;
    // temp migrated tuple count
    private MongoCollection<Document> ttc;
    // migrated tuple count
    private MongoCollection<Document> tc;
    // utilizazione
    private MongoCollection<Document> ut;
    // migrating detectors
    private MongoCollection<Document> mDetectors;
    // moved detectors
    private MongoCollection<Document> movDetectors;
    // moved detectors tmp
    private MongoCollection<Document> movDetectorsTmp;
    // announce ready task in nonSmoothMode
    private MongoCollection<Document> rdyTasks;
    // troughput
    private MongoCollection<Document> trh;



    public Database(MongoClient c, String dbName, long db_ttl) {
	MongoDatabase db = c.getDatabase(dbName);
	nt = db.getCollection("networkCost");
	nc = db.getCollection("nodesCpu");
	ec = db.getCollection("executorsCpu");
	tr = db.getCollection("traffic");
	to = db.getCollection("currentTopo");
	ne = db.getCollection("numExecutor");
	mt = db.getCollection("migTime");
	rt = db.getCollection("respTime");
	tsm = db.getCollection("tmpMigSize");
	sm = db.getCollection("totalMigSize");
	tc = db.getCollection("tupleCount");
	ttc = db.getCollection("tempTupleCount");
	ut = db.getCollection("utilization");
	trh = db.getCollection("appcost");
	mDetectors = db.getCollection("mDetectors");
	movDetectors = db.getCollection("movDetectors");
	movDetectorsTmp = db.getCollection("movDetectorsTmp");
	rdyTasks = db.getCollection("rdyTasks");
	Document index = new Document();
	index.append("date", 1);
	IndexOptions io = new IndexOptions();
	io.expireAfter(db_ttl, TimeUnit.SECONDS);
	nc.createIndex(index, io);
	ec.createIndex(index, io);
	tr.createIndex(index, io);
	to.createIndex(index, io);

    }

    public Database(MongoClient c, String dbName) {
	MongoDatabase db = c.getDatabase(dbName);
	nt = db.getCollection("networkCost");
	nc = db.getCollection("nodesCpu");
	ec = db.getCollection("executorsCpu");
	tr = db.getCollection("traffic");
	to = db.getCollection("currentTopo");
	ne = db.getCollection("numExecutor");
	mt = db.getCollection("migTime");
	rt = db.getCollection("respTime");
	tsm = db.getCollection("tmpMigSize");
	sm = db.getCollection("totalMigSize");
	tc = db.getCollection("tupleCount");
	ttc = db.getCollection("tempTupleCount");
	ut = db.getCollection("utilization");
	trh = db.getCollection("appcost");
	mDetectors = db.getCollection("mDetectors");
	movDetectors = db.getCollection("movDetectors");
	movDetectorsTmp = db.getCollection("movDetectorsTmp");
	rdyTasks = db.getCollection("rdyTasks");

    }

    public MongoCollection<Document> getMDetectors() {
	return mDetectors;
    }

    public MongoCollection<Document> getUt() {
	return ut;
    }

    public void setUt(MongoCollection<Document> ut) {
	this.ut = ut;
    }

    public MongoCollection<Document> getNc() {
	return nc;
    }

    public MongoCollection<Document> getEc() {
	return ec;
    }

    public MongoCollection<Document> getTr() {
	return tr;
    }

    public MongoCollection<Document> getTo() {
	return to;
    }

    public MongoCollection<Document> getNe() {
	return ne;
    }

    public void setNe(MongoCollection<Document> ne) {
	this.ne = ne;
    }

    public MongoCollection<Document> getMt() {
	return mt;
    }

    public void setMt(MongoCollection<Document> mt) {
	this.mt = mt;
    }

    public MongoCollection<Document> getRt() {
	return rt;
    }

    public void setRt(MongoCollection<Document> rt) {
	this.rt = rt;
    }

    public MongoCollection<Document> getTsm() {
	return tsm;
    }

    public void setTsm(MongoCollection<Document> tsm) {
	this.tsm = tsm;
    }

    public MongoCollection<Document> getSm() {
	return sm;
    }

    public void setSm(MongoCollection<Document> sm) {
	this.sm = sm;
    }

    public MongoCollection<Document> getNt() {
	return nt;
    }

    public void setNt(MongoCollection<Document> nt) {
	this.nt = nt;
    }

    public MongoCollection<Document> getTc() {
	return tc;
    }

    public void setTc(MongoCollection<Document> tc) {
	this.tc = tc;
    }

    public MongoCollection<Document> getTtc() {
	return ttc;
    }

    public void setTtc(MongoCollection<Document> ttc) {
	this.ttc = ttc;
    }

    public MongoCollection<Document> getMovDetectors() {
	return movDetectors;
    }

    public MongoCollection<Document> getMovDetectorsTmp() {
	return movDetectorsTmp;
    }

    public MongoCollection<Document> getRdyTasks() {
	return rdyTasks;
    }
    public MongoCollection<Document> getTrh() {
        return trh;
    }

}
