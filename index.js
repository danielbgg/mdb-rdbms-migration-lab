const config = require('config');
const Client = require('pg').Client;
const { MongoClient } = require("mongodb");

const credentials = {
    user: config.get('POSTGRESQL.username'),
    password: config.get('POSTGRESQL.password'),
    host: config.get('POSTGRESQL.host'),
    database: config.get('POSTGRESQL.database'),
    port: config.get('POSTGRESQL.port')
}

// connect to mongo marine trader database and print out everything in the "boats" table
async function queryPostgresql() {

    let query = " select b.id, b.name, ht.hull_type, rt.rigging_type, c.construction_materials, bt.ballast_type, d.designer, emo.model, emm.make, bu.builder, ft.type, ";
    query = query + " loa, lwl, beam, sa_reported, draft_max, i, j, p, e, spl_tps, isp, sa_fore, sa_main, sa_disp_calc, est_forestay_len, displacement, sa_disp, bal_disp, disp_len, first_built, number_built, draft_min, ";
    query = query + " hp, fuel_capacity, headroom, water, last_built, website, ksp, py, ey, bridgedeck_clearance, bruce_number ";
    query = query + " from boats b, hull_types ht, rigging_types rt, construction c, ballast_types bt, designers d, engine_models emo, engine_makes emm, builders bu, fuel_types ft      ";
    query = query + " where b.hull_type = ht.id  ";
    query = query + " and b.rigging_type = rt.id  ";
    query = query + " and b.construction = c.id ";
    query = query + " and b.ballast_type = bt.id  ";
    query = query + " and b.designer = d.id ";
    query = query + " and b.engine_model = emo.id  ";
    query = query + " and b.engine_make = emm.id  ";
    query = query + " and b.builder = bu.id  ";
    query = query + " and b.fuel_type = ft.id  ";
    query = query + " order by b.id  ";

    const postgresClient = new Client(credentials);
    await postgresClient.connect();
    const boats = await postgresClient.query(query);
    await postgresClient.end();
    var newBoats = [];
    boats.rows.forEach(boat => {

        let bb = {
            id: null,
            name: null,
            info: {},
            specs: {}
        };

        bb.id = boat.id;
        bb.name = boat.name;

        bb.info.hull_typea
        bb.info.rigging_type = boat.rigging_type;
        bb.info.construction_materials = boat.construction_materials;
        bb.info.ballast_type = boat.ballast_type;
        bb.info.designer = boat.designer
        bb.info.model = boat.model;
        bb.info.make = boat.make;
        bb.info.builder = boat.builder;
        bb.info.type = boat.type;

        bb.specs.loa = boat.loa;
        bb.specs.lwl = boat.lwl;
        bb.specs.beam = boat.beam;
        bb.specs.sa_reported = boat.sa_reported;
        bb.specs.draft_max = boat.draft_max;
        bb.specs.i = boat.i;
        bb.specs.j = boat.j;
        bb.specs.p = boat.p;
        bb.specs.e = boat.e;
        bb.specs.spl_tps = boat.spl_tps;
        bb.specs.isp = boat.isp;
        bb.specs.sa_fore = boat.sa_fore;
        bb.specs.sa_main = boat.sa_main;
        bb.specs.sa_disp_calc = boat.sa_disp_calc;
        bb.specs.est_forestay_len = boat.est_forestay_len;
        bb.specs.displacement = boat.displacement;
        bb.specs.sa_disp = boat.sa_disp;
        bb.specs.bal_disp = boat.bal_disp;
        bb.specs.disp_len = boat.disp_len;
        bb.specs.first_built = boat.first_built;
        bb.specs.number_built = boat.number_built;
        bb.specs.draft_min = boat.draft_min;
        bb.specs.hp = boat.hp;
        bb.specs.fuel_capacity = boat.fuel_capacity;
        bb.specs.headroom = boat.headroom;
        bb.specs.water = boat.water;
        bb.specs.last_built = boat.last_built;
        bb.specs.website = boat.website;
        bb.specs.ksp = boat.ksp;
        bb.specs.py = boat.py;
        bb.specs.ey = boat.ey;
        bb.specs.bridgedeck_clearance = boat.bridgedeck_clearance;
        bb.specs.bruce_number = boat.bruce_number;

        newBoats.push(bb);
    })
    console.log(newBoats.length);

    const uri = config.get('MONGODB.uri');
    const client = new MongoClient(uri);
    await client.connect();
    const dbName = config.get('MONGODB.dbName');
    const collectionName = config.get('MONGODB.collectionName');
    const database = client.db(dbName);
    const collection = database.collection(collectionName);
    try {
        const insertManyResult = await collection.insertMany(newBoats);
        console.log(`${insertManyResult.insertedCount} documents successfully inserted.\n`);
    } catch (err) {
        console.error(`Something went wrong trying to insert the new documents: ${err}\n`);
    }
    await client.close();
    return newBoats;
}

// run the function
queryPostgresql();
