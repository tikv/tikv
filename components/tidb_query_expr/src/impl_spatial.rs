// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Geospatial DE-9IM predicate functions pushed down from TiDB's spatial
//! index refine filter.
//!
//! Each predicate takes two EWKB byte strings — the stored geometry column
//! value and the query constant — and returns `1`/`0` (or NULL if either
//! argument is NULL). TiDB only pushes these down after the bbox pre-filter
//! has pruned the obvious non-matches, so they run on the surviving
//! candidates.
//!
//! ## Value layout (EWKB)
//!
//! A geometry value is a 4-byte little-endian SRID prefix followed by standard
//! OGC WKB:
//!
//! ```text
//! <srid: u32 little-endian> <standard WKB>
//! ```
//!
//! We strip the SRID prefix (TiDB has already gated the query by SRID) and
//! evaluate the relation on the raw planar coordinates.
//!
//! ## Semantics
//!
//! Relations follow the OGC DE-9IM model via the `geo` crate's `Relate`,
//! matching TiDB's reference evaluator `pkg/util/geomrel` (which uses the
//! pure-Go `simplefeatures`, the same OGC spec as GEOS/JTS/PostGIS). Boundary
//! semantics matter: a point on a polygon edge is `Within=false`,
//! `Touches=true`, `CoveredBy=true`.

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use geo::{
    Coord, Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon,
    Point, Polygon,
    algorithm::relate::{IntersectionMatrix, Relate},
};
use tidb_query_codegen::rpn_fn;
use tidb_query_common::Result;
use tidb_query_datatype::codec::data_type::*;

// OGC WKB geometry type codes (2D).
const WKB_POINT: u32 = 1;
const WKB_LINESTRING: u32 = 2;
const WKB_POLYGON: u32 = 3;
const WKB_MULTIPOINT: u32 = 4;
const WKB_MULTILINESTRING: u32 = 5;
const WKB_MULTIPOLYGON: u32 = 6;
const WKB_GEOMETRYCOLLECTION: u32 = 7;

/// Decodes an EWKB byte string into a `geo` geometry, stripping the 4-byte SRID
/// prefix and parsing the remainder as standard OGC WKB.
fn decode_ewkb(bytes: BytesRef<'_>) -> Result<Geometry<f64>> {
    if bytes.len() < 4 {
        return Err(other_err!(
            "invalid EWKB: {} bytes, shorter than the 4-byte SRID prefix",
            bytes.len()
        ));
    }
    // Skip the little-endian SRID; the remainder is standard WKB.
    let mut wkb = &bytes[4..];
    let geom = read_geometry(&mut wkb)?;
    Ok(geom)
}

/// Reads the byte-order flag (`0` = big-endian, `1` = little-endian).
fn read_is_le(buf: &mut &[u8]) -> Result<bool> {
    match buf.read_u8().map_err(truncated)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(other_err!("invalid WKB byte order: {}", other)),
    }
}

fn read_u32(buf: &mut &[u8], le: bool) -> Result<u32> {
    if le {
        buf.read_u32::<LittleEndian>()
    } else {
        buf.read_u32::<BigEndian>()
    }
    .map_err(truncated)
}

fn read_coord(buf: &mut &[u8], le: bool) -> Result<Coord<f64>> {
    let (x, y) = if le {
        (
            buf.read_f64::<LittleEndian>().map_err(truncated)?,
            buf.read_f64::<LittleEndian>().map_err(truncated)?,
        )
    } else {
        (
            buf.read_f64::<BigEndian>().map_err(truncated)?,
            buf.read_f64::<BigEndian>().map_err(truncated)?,
        )
    };
    Ok(Coord { x, y })
}

/// Caps a wire-supplied element count to what the remaining buffer could
/// possibly hold, so a corrupt length cannot trigger a huge pre-allocation.
/// `unit` is the minimum byte size of one element.
fn capped(n: u32, buf: &[u8], unit: usize) -> usize {
    (n as usize).min(buf.len() / unit.max(1) + 1)
}

fn read_linestring(buf: &mut &[u8], le: bool) -> Result<LineString<f64>> {
    let n = read_u32(buf, le)?;
    let mut coords = Vec::with_capacity(capped(n, buf, 16));
    for _ in 0..n {
        coords.push(read_coord(buf, le)?);
    }
    Ok(LineString::new(coords))
}

fn read_polygon(buf: &mut &[u8], le: bool) -> Result<Polygon<f64>> {
    let num_rings = read_u32(buf, le)?;
    if num_rings == 0 {
        return Ok(Polygon::new(LineString::new(vec![]), vec![]));
    }
    let exterior = read_linestring(buf, le)?;
    let mut interiors = Vec::with_capacity(capped(num_rings - 1, buf, 4));
    for _ in 1..num_rings {
        interiors.push(read_linestring(buf, le)?);
    }
    Ok(Polygon::new(exterior, interiors))
}

/// Reads one WKB geometry (its own byte-order byte and type code), recursing
/// into the collection types.
fn read_geometry(buf: &mut &[u8]) -> Result<Geometry<f64>> {
    let le = read_is_le(buf)?;
    let type_code = read_u32(buf, le)?;
    match type_code {
        WKB_POINT => Ok(Geometry::Point(Point::from(read_coord(buf, le)?))),
        WKB_LINESTRING => Ok(Geometry::LineString(read_linestring(buf, le)?)),
        WKB_POLYGON => Ok(Geometry::Polygon(read_polygon(buf, le)?)),
        WKB_MULTIPOINT => {
            let n = read_u32(buf, le)?;
            let mut pts = Vec::with_capacity(capped(n, buf, 21));
            for _ in 0..n {
                match read_geometry(buf)? {
                    Geometry::Point(p) => pts.push(p),
                    g => return Err(unexpected("MultiPoint", "Point", &g)),
                }
            }
            Ok(Geometry::MultiPoint(MultiPoint::new(pts)))
        }
        WKB_MULTILINESTRING => {
            let n = read_u32(buf, le)?;
            let mut lines = Vec::with_capacity(capped(n, buf, 9));
            for _ in 0..n {
                match read_geometry(buf)? {
                    Geometry::LineString(l) => lines.push(l),
                    g => return Err(unexpected("MultiLineString", "LineString", &g)),
                }
            }
            Ok(Geometry::MultiLineString(MultiLineString::new(lines)))
        }
        WKB_MULTIPOLYGON => {
            let n = read_u32(buf, le)?;
            let mut polys = Vec::with_capacity(capped(n, buf, 9));
            for _ in 0..n {
                match read_geometry(buf)? {
                    Geometry::Polygon(p) => polys.push(p),
                    g => return Err(unexpected("MultiPolygon", "Polygon", &g)),
                }
            }
            Ok(Geometry::MultiPolygon(MultiPolygon::new(polys)))
        }
        WKB_GEOMETRYCOLLECTION => {
            let n = read_u32(buf, le)?;
            let mut geoms = Vec::with_capacity(capped(n, buf, 9));
            for _ in 0..n {
                geoms.push(read_geometry(buf)?);
            }
            Ok(Geometry::GeometryCollection(GeometryCollection::new_from(
                geoms,
            )))
        }
        other => Err(other_err!(
            "unsupported WKB geometry type code: {} (only 2D OGC types 1-7 are supported)",
            other
        )),
    }
}

fn truncated(e: std::io::Error) -> tidb_query_common::error::Error {
    other_err!("truncated WKB: {}", e)
}

fn unexpected(container: &str, want: &str, got: &Geometry<f64>) -> tidb_query_common::error::Error {
    let got = match got {
        Geometry::Point(_) => "Point",
        Geometry::Line(_) => "Line",
        Geometry::LineString(_) => "LineString",
        Geometry::Polygon(_) => "Polygon",
        Geometry::MultiPoint(_) => "MultiPoint",
        Geometry::MultiLineString(_) => "MultiLineString",
        Geometry::MultiPolygon(_) => "MultiPolygon",
        Geometry::GeometryCollection(_) => "GeometryCollection",
        Geometry::Rect(_) => "Rect",
        Geometry::Triangle(_) => "Triangle",
    };
    other_err!("{} element must be a {}, found {}", container, want, got)
}

/// Decodes both operands and computes their DE-9IM intersection matrix.
fn relate(a: BytesRef<'_>, b: BytesRef<'_>) -> Result<IntersectionMatrix> {
    let ga = decode_ewkb(a)?;
    let gb = decode_ewkb(b)?;
    Ok(ga.relate(&gb))
}

/// OGC topological dimension: 0 (point), 1 (curve), 2 (surface); a collection
/// takes the max over its members.
fn geom_dimension(g: &Geometry<f64>) -> i32 {
    match g {
        Geometry::Point(_) | Geometry::MultiPoint(_) => 0,
        Geometry::Line(_) | Geometry::LineString(_) | Geometry::MultiLineString(_) => 1,
        Geometry::Polygon(_)
        | Geometry::MultiPolygon(_)
        | Geometry::Rect(_)
        | Geometry::Triangle(_) => 2,
        Geometry::GeometryCollection(gc) => gc.0.iter().map(geom_dimension).max().unwrap_or(-1),
    }
}

macro_rules! st_predicate {
    ($name:ident, $matrix_method:ident) => {
        #[rpn_fn]
        #[inline]
        pub fn $name(a: BytesRef, b: BytesRef) -> Result<Option<Int>> {
            Ok(Some(relate(a, b)?.$matrix_method() as i64))
        }
    };
}

st_predicate!(st_within, is_within);
st_predicate!(st_contains, is_contains);
st_predicate!(st_intersects, is_intersects);
st_predicate!(st_equals, is_equal_topo);
st_predicate!(st_disjoint, is_disjoint);
st_predicate!(st_touches, is_touches);
st_predicate!(st_overlaps, is_overlaps);
st_predicate!(st_covers, is_covers);
st_predicate!(st_covered_by, is_coveredby);

/// ST_Crosses is dimension-gated to match MySQL: it returns NULL unless
/// dim(a) < dim(b) or both operands are curves (lines). The `geo` crate
/// computes crosses symmetrically, so the gate is applied here.
#[rpn_fn]
#[inline]
pub fn st_crosses(a: BytesRef, b: BytesRef) -> Result<Option<Int>> {
    let ga = decode_ewkb(a)?;
    let gb = decode_ewkb(b)?;
    let (da, db) = (geom_dimension(&ga), geom_dimension(&gb));
    if !(da < db || (da == 1 && db == 1)) {
        return Ok(None);
    }
    Ok(Some(ga.relate(&gb).is_crosses() as i64))
}

#[cfg(test)]
mod tests {
    use tipb::ScalarFuncSig;

    use super::*;
    use crate::types::test_util::RpnFnScalarEvaluator;

    type Pt = (f64, f64);

    // ---- EWKB builders (SRID 0, little-endian) ----

    fn put_u32(v: &mut Vec<u8>, n: u32) {
        v.extend_from_slice(&n.to_le_bytes());
    }

    fn put_f64(v: &mut Vec<u8>, f: f64) {
        v.extend_from_slice(&f.to_le_bytes());
    }

    fn header(v: &mut Vec<u8>, type_code: u32) {
        put_u32(v, 0); // SRID 0
        v.push(1); // little-endian
        put_u32(v, type_code);
    }

    fn put_ring(v: &mut Vec<u8>, pts: &[Pt]) {
        put_u32(v, pts.len() as u32);
        for &(x, y) in pts {
            put_f64(v, x);
            put_f64(v, y);
        }
    }

    fn ewkb_point(x: f64, y: f64) -> Vec<u8> {
        let mut v = Vec::new();
        header(&mut v, WKB_POINT);
        put_f64(&mut v, x);
        put_f64(&mut v, y);
        v
    }

    fn ewkb_linestring(pts: &[Pt]) -> Vec<u8> {
        let mut v = Vec::new();
        header(&mut v, WKB_LINESTRING);
        put_ring(&mut v, pts);
        v
    }

    fn ewkb_polygon(rings: &[&[Pt]]) -> Vec<u8> {
        let mut v = Vec::new();
        header(&mut v, WKB_POLYGON);
        put_u32(&mut v, rings.len() as u32);
        for r in rings {
            put_ring(&mut v, r);
        }
        v
    }

    // Predicate sigs in the column order used by the expected-value arrays.
    const SIGS: [ScalarFuncSig; 10] = [
        ScalarFuncSig::StWithin,
        ScalarFuncSig::StContains,
        ScalarFuncSig::StIntersects,
        ScalarFuncSig::StEquals,
        ScalarFuncSig::StDisjoint,
        ScalarFuncSig::StTouches,
        ScalarFuncSig::StCrosses,
        ScalarFuncSig::StOverlaps,
        ScalarFuncSig::StCovers,
        ScalarFuncSig::StCoveredBy,
    ];

    fn eval(sig: ScalarFuncSig, a: &[u8], b: &[u8]) -> Option<Int> {
        RpnFnScalarEvaluator::new()
            .push_param(Some(a.to_vec()))
            .push_param(Some(b.to_vec()))
            .evaluate(sig)
            .unwrap()
    }

    /// The reference corpus from `tikv-pushdown-handoff.md`, generated by
    /// TiDB's `pkg/util/geomrel` (simplefeatures). Columns follow `SIGS`
    /// order: within, contains, intersects, equals, disjoint, touches,
    /// crosses, overlaps, covers, coveredby.
    ///
    /// Expected values are `Option<i64>` so the `ST_Crosses` column can be
    /// `None`: unlike the other predicates (which mirror raw DE-9IM), TiDB's
    /// `ST_Crosses` follows MySQL and is NULL unless `dim(a) < dim(b)` or both
    /// operands are curves — see `st_crosses`. So the equal-dimension polygon
    /// pairs below expect `None` for crosses rather than the raw geomrel `0`.
    #[test]
    fn test_de9im_corpus() {
        let poly4 = ewkb_polygon(&[&[(0., 0.), (4., 0.), (4., 4.), (0., 4.), (0., 0.)]]);
        let poly_a = ewkb_polygon(&[&[(0., 0.), (2., 0.), (2., 2.), (0., 2.), (0., 0.)]]);
        let poly_b = ewkb_polygon(&[&[(1., 1.), (3., 1.), (3., 3.), (1., 3.), (1., 1.)]]);
        let poly_unit = ewkb_polygon(&[&[(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]]);
        let poly_far = ewkb_polygon(&[&[(5., 5.), (6., 5.), (6., 6.), (5., 6.), (5., 5.)]]);

        // y = Some(v); n = None. Crosses (column 6) is None for the equal-
        // dimension polygon pairs (dim(a) not < dim(b)).
        let y = Some;
        let n: Option<i64> = None;

        // (geometry a, geometry b, expected per-predicate result in SIGS order)
        type Case = (Vec<u8>, Vec<u8>, [Option<i64>; 10]);
        let cases: Vec<Case> = vec![
            // interior point (dim 0 < 2, so crosses is gated-in: 0)
            (
                ewkb_point(2., 2.),
                poly4.clone(),
                [y(1), y(0), y(1), y(0), y(0), y(0), y(0), y(0), y(0), y(1)],
            ),
            // outside point
            (
                ewkb_point(5., 5.),
                poly4.clone(),
                [y(0), y(0), y(0), y(0), y(1), y(0), y(0), y(0), y(0), y(0)],
            ),
            // boundary corner point
            (
                ewkb_point(0., 0.),
                poly4.clone(),
                [y(0), y(0), y(1), y(0), y(0), y(1), y(0), y(0), y(0), y(1)],
            ),
            // diagonal line across the polygon (dim 1 < 2, crosses gated-in: 0)
            (
                ewkb_linestring(&[(0., 0.), (4., 4.)]),
                poly4.clone(),
                [y(1), y(0), y(1), y(0), y(0), y(0), y(0), y(0), y(0), y(1)],
            ),
            // partially overlapping polygons (crosses NULL: equal dimension)
            (
                poly_a,
                poly_b,
                [y(0), y(0), y(1), y(0), y(0), y(0), n, y(1), y(0), y(0)],
            ),
            // equal polygons (crosses NULL: equal dimension)
            (
                poly4.clone(),
                poly4.clone(),
                [y(1), y(1), y(1), y(1), y(0), y(0), n, y(0), y(1), y(1)],
            ),
            // disjoint polygons (crosses NULL: equal dimension)
            (
                poly_unit,
                poly_far,
                [y(0), y(0), y(0), y(0), y(1), y(0), n, y(0), y(0), y(0)],
            ),
        ];

        for (i, (a, b, expected)) in cases.iter().enumerate() {
            for (j, sig) in SIGS.iter().enumerate() {
                let out = eval(*sig, a, b);
                assert_eq!(out, expected[j], "case {} {:?}: got {:?}", i, sig, out);
            }
        }
    }

    #[test]
    fn test_null_propagation() {
        let pt = ewkb_point(1., 1.);
        for sig in SIGS {
            let a_null = RpnFnScalarEvaluator::new()
                .push_param(Option::<Bytes>::None)
                .push_param(Some(pt.clone()))
                .evaluate::<Int>(sig)
                .unwrap();
            assert_eq!(a_null, None, "{:?}", sig);

            let b_null = RpnFnScalarEvaluator::new()
                .push_param(Some(pt.clone()))
                .push_param(Option::<Bytes>::None)
                .evaluate::<Int>(sig)
                .unwrap();
            assert_eq!(b_null, None, "{:?}", sig);
        }
    }

    #[test]
    fn test_malformed_ewkb_errors() {
        let valid = ewkb_point(1., 1.);
        let mut truncated_coord = ewkb_point(1., 1.);
        truncated_coord.truncate(truncated_coord.len() - 4);

        let bad_inputs: Vec<Vec<u8>> = vec![
            vec![],                                // empty
            vec![0, 0, 0],                         // shorter than the SRID prefix
            vec![0, 0, 0, 0],                      // SRID only, no WKB
            vec![0, 0, 0, 0, 2, 1, 0, 0, 0],       // invalid byte-order byte
            vec![0, 0, 0, 0, 1, 0xE7, 0x03, 0, 0], // unsupported type code (999)
            truncated_coord,                       // truncated point coordinate
        ];
        for bad in bad_inputs {
            let res = RpnFnScalarEvaluator::new()
                .push_param(Some(bad.clone()))
                .push_param(Some(valid.clone()))
                .evaluate::<Int>(ScalarFuncSig::StIntersects);
            assert!(res.is_err(), "expected error for malformed input {:?}", bad);
        }
    }

    #[test]
    fn test_decode_ewkb_layout() {
        // POINT(2 2), SRID 0, little-endian — the worked example from the doc.
        let le = vec![
            0, 0, 0, 0,    // SRID 0
            0x01, // little-endian
            0x01, 0, 0, 0, // type = 1 (Point)
            0, 0, 0, 0, 0, 0, 0, 0x40, // x = 2.0
            0, 0, 0, 0, 0, 0, 0, 0x40, // y = 2.0
        ];
        match decode_ewkb(&le).unwrap() {
            Geometry::Point(p) => {
                assert_eq!(p.x(), 2.0);
                assert_eq!(p.y(), 2.0);
            }
            g => panic!("expected Point, got {:?}", g),
        }

        // The same point with a big-endian WKB body must decode identically.
        let be = vec![
            0, 0, 0, 0,    // SRID 0 (the prefix itself is always little-endian)
            0x00, // big-endian
            0, 0, 0, 0x01, // type = 1 (Point)
            0x40, 0, 0, 0, 0, 0, 0, 0, // x = 2.0
            0x40, 0, 0, 0, 0, 0, 0, 0, // y = 2.0
        ];
        match decode_ewkb(&be).unwrap() {
            Geometry::Point(p) => {
                assert_eq!(p.x(), 2.0);
                assert_eq!(p.y(), 2.0);
            }
            g => panic!("expected Point, got {:?}", g),
        }
    }
}
