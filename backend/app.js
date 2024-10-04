const express = require('express');
const mysql = require('mysql2');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const app = express();
const port = 8000;

// Configurar cors para permitir solicitudes desde http://localhost:3000
app.use(cors());

// Conectar a la base de datos MySQL
// const db = mysql.createConnection({
//   host: 'mysql-db.cpmoyou2oi10.us-west-1.rds.amazonaws.com',
//   user: 'paydelimon',
//   password: 'limon123',
//   database: 'Google',
//   port: 3306
// });

// db.connect((err) => {
//   if (err) {
//     console.error('Error al conectar a la base de datos:', err.message);
//   } else {
//     console.log('Conectado a la base de datos MySQL.');
//   }
// });
const db = new sqlite3.Database('./quantumdbfinal.db', (err) => {
  if (err) {
    console.error('Error al conectar a la base de datos SQLite:', err.message);
  } else {
    console.log('Conectado a la base de datos SQLite.');
  }
});

// Middleware para parsear JSON
app.use(express.json());

// Rutas para interactuar con la base de datos
app.get('/data', (req, res) => {
  const { stadium_cercano, category, distancia_millas, price } = req.query;

  let sql = 'SELECT * FROM sites WHERE 1=1';
  const params = [];

  // Condiciones opcionales basadas en los parámetros recibidos
  if (stadium_cercano) {
    sql += ' AND stadium_cercano = ?';
    params.push(stadium_cercano);
  }
  if (category) {
    sql += ' AND category LIKE ?';
    params.push(`%${category}%`);
  }
  if (distancia_millas) {
    sql += ' AND distancia_millas < ?';
    params.push(distancia_millas);
  }
  if (price) {
    sql += ' AND price = ?';
    params.push(price);
  }

  sql += ' ORDER BY calificacion_ajustada_predicha DESC LIMIT 10';

  console.log(sql, params)

  // Ejecutar la consulta SQL
  // db.query(sql, params, (err, rows) => {
  //   if (err) {
  //     res.status(400).json({ error: err.message });
  //     return;
  //   }
  //   res.json({ data: rows });
  // });

  // Ejecutar la consulta SQL en SQLite
  db.all(sql, params, (err, rows) => {
   if (err) {
     res.status(400).json({ error: err.message });
     return;
   }
   res.json({ data: rows });
 });

});

// Iniciar el servidor
app.listen(port, () => {
  console.log(`Servidor corriendo en http://localhost:${port}`);
});
