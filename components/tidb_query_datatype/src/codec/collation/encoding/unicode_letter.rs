// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

/// In order to keep the same behavoir as TiDB that uses go standard library to
/// implement lower and upper functions. Below code is ported from https://github.com/golang/go/blob/go1.21.3/src/unicode/letter.go.
const UPPER_CASE: usize = 0;
const LOWER_CASE: usize = 1;
const TITLE_CASE: usize = 2;
const MAX_CASE: usize = 3;

const MAX_ASCII: i32 = 0x7F;
const MAX_RUNE: i32 = 0x10FFFF;
const REPLACEMENT_CHAR: i32 = 0xFFFD;

const UPPER_LOWER: i32 = MAX_RUNE + 1;

static CASE_TABLE: &[(i32, i32, [i32; MAX_CASE])] = &[
    (0x0041, 0x005A, [0, 32, 0]),
    (0x0061, 0x007A, [-32, 0, -32]),
    (0x00B5, 0x00B5, [743, 0, 743]),
    (0x00C0, 0x00D6, [0, 32, 0]),
    (0x00D8, 0x00DE, [0, 32, 0]),
    (0x00E0, 0x00F6, [-32, 0, -32]),
    (0x00F8, 0x00FE, [-32, 0, -32]),
    (0x00FF, 0x00FF, [121, 0, 121]),
    (0x0100, 0x012F, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x0130, 0x0130, [0, -199, 0]),
    (0x0131, 0x0131, [-232, 0, -232]),
    (0x0132, 0x0137, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x0139, 0x0148, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x014A, 0x0177, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x0178, 0x0178, [0, -121, 0]),
    (0x0179, 0x017E, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x017F, 0x017F, [-300, 0, -300]),
    (0x0180, 0x0180, [195, 0, 195]),
    (0x0181, 0x0181, [0, 210, 0]),
    (0x0182, 0x0185, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x0186, 0x0186, [0, 206, 0]),
    (0x0187, 0x0188, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x0189, 0x018A, [0, 205, 0]),
    (0x018B, 0x018C, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x018E, 0x018E, [0, 79, 0]),
    (0x018F, 0x018F, [0, 202, 0]),
    (0x0190, 0x0190, [0, 203, 0]),
    (0x0191, 0x0192, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x0193, 0x0193, [0, 205, 0]),
    (0x0194, 0x0194, [0, 207, 0]),
    (0x0195, 0x0195, [97, 0, 97]),
    (0x0196, 0x0196, [0, 211, 0]),
    (0x0197, 0x0197, [0, 209, 0]),
    (0x0198, 0x0199, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x019A, 0x019A, [163, 0, 163]),
    (0x019C, 0x019C, [0, 211, 0]),
    (0x019D, 0x019D, [0, 213, 0]),
    (0x019E, 0x019E, [130, 0, 130]),
    (0x019F, 0x019F, [0, 214, 0]),
    (0x01A0, 0x01A5, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x01A6, 0x01A6, [0, 218, 0]),
    (0x01A7, 0x01A8, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x01A9, 0x01A9, [0, 218, 0]),
    (0x01AC, 0x01AD, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x01AE, 0x01AE, [0, 218, 0]),
    (0x01AF, 0x01B0, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x01B1, 0x01B2, [0, 217, 0]),
    (0x01B3, 0x01B6, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x01B7, 0x01B7, [0, 219, 0]),
    (0x01B8, 0x01B9, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x01BC, 0x01BD, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x01BF, 0x01BF, [56, 0, 56]),
    (0x01C4, 0x01C4, [0, 2, 1]),
    (0x01C5, 0x01C5, [-1, 1, 0]),
    (0x01C6, 0x01C6, [-2, 0, -1]),
    (0x01C7, 0x01C7, [0, 2, 1]),
    (0x01C8, 0x01C8, [-1, 1, 0]),
    (0x01C9, 0x01C9, [-2, 0, -1]),
    (0x01CA, 0x01CA, [0, 2, 1]),
    (0x01CB, 0x01CB, [-1, 1, 0]),
    (0x01CC, 0x01CC, [-2, 0, -1]),
    (0x01CD, 0x01DC, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x01DD, 0x01DD, [-79, 0, -79]),
    (0x01DE, 0x01EF, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x01F1, 0x01F1, [0, 2, 1]),
    (0x01F2, 0x01F2, [-1, 1, 0]),
    (0x01F3, 0x01F3, [-2, 0, -1]),
    (0x01F4, 0x01F5, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x01F6, 0x01F6, [0, -97, 0]),
    (0x01F7, 0x01F7, [0, -56, 0]),
    (0x01F8, 0x021F, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x0220, 0x0220, [0, -130, 0]),
    (0x0222, 0x0233, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x023A, 0x023A, [0, 10795, 0]),
    (0x023B, 0x023C, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x023D, 0x023D, [0, -163, 0]),
    (0x023E, 0x023E, [0, 10792, 0]),
    (0x023F, 0x0240, [10815, 0, 10815]),
    (0x0241, 0x0242, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x0243, 0x0243, [0, -195, 0]),
    (0x0244, 0x0244, [0, 69, 0]),
    (0x0245, 0x0245, [0, 71, 0]),
    (0x0246, 0x024F, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x0250, 0x0250, [10783, 0, 10783]),
    (0x0251, 0x0251, [10780, 0, 10780]),
    (0x0252, 0x0252, [10782, 0, 10782]),
    (0x0253, 0x0253, [-210, 0, -210]),
    (0x0254, 0x0254, [-206, 0, -206]),
    (0x0256, 0x0257, [-205, 0, -205]),
    (0x0259, 0x0259, [-202, 0, -202]),
    (0x025B, 0x025B, [-203, 0, -203]),
    (0x025C, 0x025C, [42319, 0, 42319]),
    (0x0260, 0x0260, [-205, 0, -205]),
    (0x0261, 0x0261, [42315, 0, 42315]),
    (0x0263, 0x0263, [-207, 0, -207]),
    (0x0265, 0x0265, [42280, 0, 42280]),
    (0x0266, 0x0266, [42308, 0, 42308]),
    (0x0268, 0x0268, [-209, 0, -209]),
    (0x0269, 0x0269, [-211, 0, -211]),
    (0x026A, 0x026A, [42308, 0, 42308]),
    (0x026B, 0x026B, [10743, 0, 10743]),
    (0x026C, 0x026C, [42305, 0, 42305]),
    (0x026F, 0x026F, [-211, 0, -211]),
    (0x0271, 0x0271, [10749, 0, 10749]),
    (0x0272, 0x0272, [-213, 0, -213]),
    (0x0275, 0x0275, [-214, 0, -214]),
    (0x027D, 0x027D, [10727, 0, 10727]),
    (0x0280, 0x0280, [-218, 0, -218]),
    (0x0282, 0x0282, [42307, 0, 42307]),
    (0x0283, 0x0283, [-218, 0, -218]),
    (0x0287, 0x0287, [42282, 0, 42282]),
    (0x0288, 0x0288, [-218, 0, -218]),
    (0x0289, 0x0289, [-69, 0, -69]),
    (0x028A, 0x028B, [-217, 0, -217]),
    (0x028C, 0x028C, [-71, 0, -71]),
    (0x0292, 0x0292, [-219, 0, -219]),
    (0x029D, 0x029D, [42261, 0, 42261]),
    (0x029E, 0x029E, [42258, 0, 42258]),
    (0x0345, 0x0345, [84, 0, 84]),
    (0x0370, 0x0373, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x0376, 0x0377, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x037B, 0x037D, [130, 0, 130]),
    (0x037F, 0x037F, [0, 116, 0]),
    (0x0386, 0x0386, [0, 38, 0]),
    (0x0388, 0x038A, [0, 37, 0]),
    (0x038C, 0x038C, [0, 64, 0]),
    (0x038E, 0x038F, [0, 63, 0]),
    (0x0391, 0x03A1, [0, 32, 0]),
    (0x03A3, 0x03AB, [0, 32, 0]),
    (0x03AC, 0x03AC, [-38, 0, -38]),
    (0x03AD, 0x03AF, [-37, 0, -37]),
    (0x03B1, 0x03C1, [-32, 0, -32]),
    (0x03C2, 0x03C2, [-31, 0, -31]),
    (0x03C3, 0x03CB, [-32, 0, -32]),
    (0x03CC, 0x03CC, [-64, 0, -64]),
    (0x03CD, 0x03CE, [-63, 0, -63]),
    (0x03CF, 0x03CF, [0, 8, 0]),
    (0x03D0, 0x03D0, [-62, 0, -62]),
    (0x03D1, 0x03D1, [-57, 0, -57]),
    (0x03D5, 0x03D5, [-47, 0, -47]),
    (0x03D6, 0x03D6, [-54, 0, -54]),
    (0x03D7, 0x03D7, [-8, 0, -8]),
    (0x03D8, 0x03EF, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x03F0, 0x03F0, [-86, 0, -86]),
    (0x03F1, 0x03F1, [-80, 0, -80]),
    (0x03F2, 0x03F2, [7, 0, 7]),
    (0x03F3, 0x03F3, [-116, 0, -116]),
    (0x03F4, 0x03F4, [0, -60, 0]),
    (0x03F5, 0x03F5, [-96, 0, -96]),
    (0x03F7, 0x03F8, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x03F9, 0x03F9, [0, -7, 0]),
    (0x03FA, 0x03FB, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x03FD, 0x03FF, [0, -130, 0]),
    (0x0400, 0x040F, [0, 80, 0]),
    (0x0410, 0x042F, [0, 32, 0]),
    (0x0430, 0x044F, [-32, 0, -32]),
    (0x0450, 0x045F, [-80, 0, -80]),
    (0x0460, 0x0481, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x048A, 0x04BF, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x04C0, 0x04C0, [0, 15, 0]),
    (0x04C1, 0x04CE, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x04CF, 0x04CF, [-15, 0, -15]),
    (0x04D0, 0x052F, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x0531, 0x0556, [0, 48, 0]),
    (0x0561, 0x0586, [-48, 0, -48]),
    (0x10A0, 0x10C5, [0, 7264, 0]),
    (0x10C7, 0x10C7, [0, 7264, 0]),
    (0x10CD, 0x10CD, [0, 7264, 0]),
    (0x10D0, 0x10FA, [3008, 0, 0]),
    (0x10FD, 0x10FF, [3008, 0, 0]),
    (0x13A0, 0x13EF, [0, 38864, 0]),
    (0x13F0, 0x13F5, [0, 8, 0]),
    (0x13F8, 0x13FD, [-8, 0, -8]),
    (0x1C80, 0x1C80, [-6254, 0, -6254]),
    (0x1C81, 0x1C81, [-6253, 0, -6253]),
    (0x1C82, 0x1C82, [-6244, 0, -6244]),
    (0x1C83, 0x1C84, [-6242, 0, -6242]),
    (0x1C85, 0x1C85, [-6243, 0, -6243]),
    (0x1C86, 0x1C86, [-6236, 0, -6236]),
    (0x1C87, 0x1C87, [-6181, 0, -6181]),
    (0x1C88, 0x1C88, [35266, 0, 35266]),
    (0x1C90, 0x1CBA, [0, -3008, 0]),
    (0x1CBD, 0x1CBF, [0, -3008, 0]),
    (0x1D79, 0x1D79, [35332, 0, 35332]),
    (0x1D7D, 0x1D7D, [3814, 0, 3814]),
    (0x1D8E, 0x1D8E, [35384, 0, 35384]),
    (0x1E00, 0x1E95, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x1E9B, 0x1E9B, [-59, 0, -59]),
    (0x1E9E, 0x1E9E, [0, -7615, 0]),
    (0x1EA0, 0x1EFF, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x1F00, 0x1F07, [8, 0, 8]),
    (0x1F08, 0x1F0F, [0, -8, 0]),
    (0x1F10, 0x1F15, [8, 0, 8]),
    (0x1F18, 0x1F1D, [0, -8, 0]),
    (0x1F20, 0x1F27, [8, 0, 8]),
    (0x1F28, 0x1F2F, [0, -8, 0]),
    (0x1F30, 0x1F37, [8, 0, 8]),
    (0x1F38, 0x1F3F, [0, -8, 0]),
    (0x1F40, 0x1F45, [8, 0, 8]),
    (0x1F48, 0x1F4D, [0, -8, 0]),
    (0x1F51, 0x1F51, [8, 0, 8]),
    (0x1F53, 0x1F53, [8, 0, 8]),
    (0x1F55, 0x1F55, [8, 0, 8]),
    (0x1F57, 0x1F57, [8, 0, 8]),
    (0x1F59, 0x1F59, [0, -8, 0]),
    (0x1F5B, 0x1F5B, [0, -8, 0]),
    (0x1F5D, 0x1F5D, [0, -8, 0]),
    (0x1F5F, 0x1F5F, [0, -8, 0]),
    (0x1F60, 0x1F67, [8, 0, 8]),
    (0x1F68, 0x1F6F, [0, -8, 0]),
    (0x1F70, 0x1F71, [74, 0, 74]),
    (0x1F72, 0x1F75, [86, 0, 86]),
    (0x1F76, 0x1F77, [100, 0, 100]),
    (0x1F78, 0x1F79, [128, 0, 128]),
    (0x1F7A, 0x1F7B, [112, 0, 112]),
    (0x1F7C, 0x1F7D, [126, 0, 126]),
    (0x1F80, 0x1F87, [8, 0, 8]),
    (0x1F88, 0x1F8F, [0, -8, 0]),
    (0x1F90, 0x1F97, [8, 0, 8]),
    (0x1F98, 0x1F9F, [0, -8, 0]),
    (0x1FA0, 0x1FA7, [8, 0, 8]),
    (0x1FA8, 0x1FAF, [0, -8, 0]),
    (0x1FB0, 0x1FB1, [8, 0, 8]),
    (0x1FB3, 0x1FB3, [9, 0, 9]),
    (0x1FB8, 0x1FB9, [0, -8, 0]),
    (0x1FBA, 0x1FBB, [0, -74, 0]),
    (0x1FBC, 0x1FBC, [0, -9, 0]),
    (0x1FBE, 0x1FBE, [-7205, 0, -7205]),
    (0x1FC3, 0x1FC3, [9, 0, 9]),
    (0x1FC8, 0x1FCB, [0, -86, 0]),
    (0x1FCC, 0x1FCC, [0, -9, 0]),
    (0x1FD0, 0x1FD1, [8, 0, 8]),
    (0x1FD8, 0x1FD9, [0, -8, 0]),
    (0x1FDA, 0x1FDB, [0, -100, 0]),
    (0x1FE0, 0x1FE1, [8, 0, 8]),
    (0x1FE5, 0x1FE5, [7, 0, 7]),
    (0x1FE8, 0x1FE9, [0, -8, 0]),
    (0x1FEA, 0x1FEB, [0, -112, 0]),
    (0x1FEC, 0x1FEC, [0, -7, 0]),
    (0x1FF3, 0x1FF3, [9, 0, 9]),
    (0x1FF8, 0x1FF9, [0, -128, 0]),
    (0x1FFA, 0x1FFB, [0, -126, 0]),
    (0x1FFC, 0x1FFC, [0, -9, 0]),
    (0x2126, 0x2126, [0, -7517, 0]),
    (0x212A, 0x212A, [0, -8383, 0]),
    (0x212B, 0x212B, [0, -8262, 0]),
    (0x2132, 0x2132, [0, 28, 0]),
    (0x214E, 0x214E, [-28, 0, -28]),
    (0x2160, 0x216F, [0, 16, 0]),
    (0x2170, 0x217F, [-16, 0, -16]),
    (0x2183, 0x2184, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x24B6, 0x24CF, [0, 26, 0]),
    (0x24D0, 0x24E9, [-26, 0, -26]),
    (0x2C00, 0x2C2F, [0, 48, 0]),
    (0x2C30, 0x2C5F, [-48, 0, -48]),
    (0x2C60, 0x2C61, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x2C62, 0x2C62, [0, -10743, 0]),
    (0x2C63, 0x2C63, [0, -3814, 0]),
    (0x2C64, 0x2C64, [0, -10727, 0]),
    (0x2C65, 0x2C65, [-10795, 0, -10795]),
    (0x2C66, 0x2C66, [-10792, 0, -10792]),
    (0x2C67, 0x2C6C, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x2C6D, 0x2C6D, [0, -10780, 0]),
    (0x2C6E, 0x2C6E, [0, -10749, 0]),
    (0x2C6F, 0x2C6F, [0, -10783, 0]),
    (0x2C70, 0x2C70, [0, -10782, 0]),
    (0x2C72, 0x2C73, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x2C75, 0x2C76, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x2C7E, 0x2C7F, [0, -10815, 0]),
    (0x2C80, 0x2CE3, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x2CEB, 0x2CEE, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x2CF2, 0x2CF3, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0x2D00, 0x2D25, [-7264, 0, -7264]),
    (0x2D27, 0x2D27, [-7264, 0, -7264]),
    (0x2D2D, 0x2D2D, [-7264, 0, -7264]),
    (0xA640, 0xA66D, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0xA680, 0xA69B, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0xA722, 0xA72F, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0xA732, 0xA76F, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0xA779, 0xA77C, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0xA77D, 0xA77D, [0, -35332, 0]),
    (0xA77E, 0xA787, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0xA78B, 0xA78C, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0xA78D, 0xA78D, [0, -42280, 0]),
    (0xA790, 0xA793, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0xA794, 0xA794, [48, 0, 48]),
    (0xA796, 0xA7A9, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0xA7AA, 0xA7AA, [0, -42308, 0]),
    (0xA7AB, 0xA7AB, [0, -42319, 0]),
    (0xA7AC, 0xA7AC, [0, -42315, 0]),
    (0xA7AD, 0xA7AD, [0, -42305, 0]),
    (0xA7AE, 0xA7AE, [0, -42308, 0]),
    (0xA7B0, 0xA7B0, [0, -42258, 0]),
    (0xA7B1, 0xA7B1, [0, -42282, 0]),
    (0xA7B2, 0xA7B2, [0, -42261, 0]),
    (0xA7B3, 0xA7B3, [0, 928, 0]),
    (0xA7B4, 0xA7C3, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0xA7C4, 0xA7C4, [0, -48, 0]),
    (0xA7C5, 0xA7C5, [0, -42307, 0]),
    (0xA7C6, 0xA7C6, [0, -35384, 0]),
    (0xA7C7, 0xA7CA, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0xA7D0, 0xA7D1, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0xA7D6, 0xA7D9, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0xA7F5, 0xA7F6, [UPPER_LOWER, UPPER_LOWER, UPPER_LOWER]),
    (0xAB53, 0xAB53, [-928, 0, -928]),
    (0xAB70, 0xABBF, [-38864, 0, -38864]),
    (0xFF21, 0xFF3A, [0, 32, 0]),
    (0xFF41, 0xFF5A, [-32, 0, -32]),
    (0x10400, 0x10427, [0, 40, 0]),
    (0x10428, 0x1044F, [-40, 0, -40]),
    (0x104B0, 0x104D3, [0, 40, 0]),
    (0x104D8, 0x104FB, [-40, 0, -40]),
    (0x10570, 0x1057A, [0, 39, 0]),
    (0x1057C, 0x1058A, [0, 39, 0]),
    (0x1058C, 0x10592, [0, 39, 0]),
    (0x10594, 0x10595, [0, 39, 0]),
    (0x10597, 0x105A1, [-39, 0, -39]),
    (0x105A3, 0x105B1, [-39, 0, -39]),
    (0x105B3, 0x105B9, [-39, 0, -39]),
    (0x105BB, 0x105BC, [-39, 0, -39]),
    (0x10C80, 0x10CB2, [0, 64, 0]),
    (0x10CC0, 0x10CF2, [-64, 0, -64]),
    (0x118A0, 0x118BF, [0, 32, 0]),
    (0x118C0, 0x118DF, [-32, 0, -32]),
    (0x16E40, 0x16E5F, [0, 32, 0]),
    (0x16E60, 0x16E7F, [-32, 0, -32]),
    (0x1E900, 0x1E921, [0, 34, 0]),
    (0x1E922, 0x1E943, [-34, 0, -34]),
];

fn to_case(case: usize, ch: i32) -> i32 {
    if case >= MAX_CASE {
        return REPLACEMENT_CHAR;
    }
    // binary search over ranges
    let mut lo = 0;
    let mut hi = CASE_TABLE.len();
    while lo < hi {
        let m = lo + (hi - lo) / 2;
        let cr = CASE_TABLE[m];
        if cr.0 <= ch && ch <= cr.1 {
            let delta = cr.2[case];
            if delta > MAX_RUNE {
                // In an Upper-Lower sequence, which always starts with
                // an UpperCase letter, the real deltas always look like:
                // 	{0, 1, 0}    UpperCase (Lower is next)
                // 	{-1, 0, -1}  LowerCase (Upper, Title are previous)
                // The characters at even offsets from the beginning of the
                // sequence are upper case; the ones at odd offsets are lower.
                // The correct mapping can be done by clearing or setting the low
                // bit in the sequence offset.
                // The constants UpperCase and TitleCase are even while LowerCase
                // is odd so we take the low bit from case.
                return cr.0 + (((ch - cr.0) & !1) | (case as i32 & 1));
            }
            return ch + delta;
        }
        if ch < cr.0 {
            hi = m;
        } else {
            lo = m + 1;
        }
    }
    ch
}

pub fn unicode_to_upper(ch: char) -> Option<char> {
    let mut r = ch as i32;
    if r < MAX_ASCII {
        if 'a' as i32 <= r && r <= 'z' as i32 {
            r -= ('a' as i32) - ('A' as i32);
        }
        char::from_u32(r as u32)
    } else {
        char::from_u32(to_case(UPPER_CASE, r) as u32)
    }
}

pub fn unicode_to_lower(ch: char) -> Option<char> {
    let mut r = ch as i32;
    if r < MAX_ASCII {
        if 'A' as i32 <= r && r <= 'Z' as i32 {
            r += ('a' as i32) - ('A' as i32);
        }
        char::from_u32(r as u32)
    } else {
        char::from_u32(to_case(LOWER_CASE, r) as u32)
    }
}

pub fn unicode_to_title(ch: char) -> Option<char> {
    let mut r = ch as i32;
    if r < MAX_ASCII {
        if 'a' as i32 <= r && r <= 'z' as i32 {
            r -= ('a' as i32) - ('A' as i32);
        }
        char::from_u32(r as u32)
    } else {
        char::from_u32(to_case(TITLE_CASE, r) as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static CASE_TEST: &[(usize, u32, u32)] = &[
        // ASCII (special-cased so test carefully)
        (UPPER_CASE, '\n' as u32, '\n' as u32),
        (UPPER_CASE, 'a' as u32, 'A' as u32),
        (UPPER_CASE, 'A' as u32, 'A' as u32),
        (UPPER_CASE, '7' as u32, '7' as u32),
        (LOWER_CASE, '\n' as u32, '\n' as u32),
        (LOWER_CASE, 'a' as u32, 'a' as u32),
        (LOWER_CASE, 'A' as u32, 'a' as u32),
        (LOWER_CASE, '7' as u32, '7' as u32),
        (TITLE_CASE, '\n' as u32, '\n' as u32),
        (TITLE_CASE, 'a' as u32, 'A' as u32),
        (TITLE_CASE, 'A' as u32, 'A' as u32),
        (TITLE_CASE, '7' as u32, '7' as u32),
        // Latin-1: easy to read the tests!
        (UPPER_CASE, 0x80, 0x80),
        (UPPER_CASE, 'Å' as u32, 'Å' as u32),
        (UPPER_CASE, 'å' as u32, 'Å' as u32),
        (LOWER_CASE, 0x80, 0x80),
        (LOWER_CASE, 'Å' as u32, 'å' as u32),
        (LOWER_CASE, 'å' as u32, 'å' as u32),
        (TITLE_CASE, 0x80, 0x80),
        (TITLE_CASE, 'Å' as u32, 'Å' as u32),
        (TITLE_CASE, 'å' as u32, 'Å' as u32),
        // 0131;LATIN SMALL LETTER DOTLESS I;Ll;0;L;;;;;N;;;0049;;0049
        (UPPER_CASE, 0x0130, 'İ' as u32),
        (LOWER_CASE, 0x0130, 'i' as u32),
        (UPPER_CASE, 0x0131, 'I' as u32),
        (LOWER_CASE, 0x0131, 0x0131),
        (TITLE_CASE, 0x0131, 'I' as u32),
        // 0133;LATIN SMALL LIGATURE IJ;Ll;0;L;<compat> 0069 006A;;;;N;LATIN SMALL LETTER I
        // J;;0132;;0132
        (UPPER_CASE, 0x0133, 0x0132),
        (LOWER_CASE, 0x0133, 0x0133),
        (TITLE_CASE, 0x0133, 0x0132),
        // 212A;KELVIN SIGN;Lu;0;L;004B;;;;N;DEGREES KELVIN;;;006B;
        (UPPER_CASE, 0x212A, 0x212A),
        (LOWER_CASE, 0x212A, 'k' as u32),
        (TITLE_CASE, 0x212A, 0x212A),
        // From an UpperLower sequence
        // A640;CYRILLIC CAPITAL LETTER ZEMLYA;Lu;0;L;;;;;N;;;;A641;
        (UPPER_CASE, 0xA640, 0xA640),
        (LOWER_CASE, 0xA640, 0xA641),
        (TITLE_CASE, 0xA640, 0xA640),
        // A641;CYRILLIC SMALL LETTER ZEMLYA;Ll;0;L;;;;;N;;;A640;;A640
        (UPPER_CASE, 0xA641, 0xA640),
        (LOWER_CASE, 0xA641, 0xA641),
        (TITLE_CASE, 0xA641, 0xA640),
        // A64E;CYRILLIC CAPITAL LETTER NEUTRAL YER;Lu;0;L;;;;;N;;;;A64F;
        (UPPER_CASE, 0xA64E, 0xA64E),
        (LOWER_CASE, 0xA64E, 0xA64F),
        (TITLE_CASE, 0xA64E, 0xA64E),
        // A65F;CYRILLIC SMALL LETTER YN;Ll;0;L;;;;;N;;;A65E;;A65E
        (UPPER_CASE, 0xA65F, 0xA65E),
        (LOWER_CASE, 0xA65F, 0xA65F),
        (TITLE_CASE, 0xA65F, 0xA65E),
        // From another UpperLower sequence
        // 0139;LATIN CAPITAL LETTER L WITH ACUTE;Lu;0;L;004C 0301;;;;N;LATIN CAPITAL LETTER L
        // ACUTE;;;013A;
        (UPPER_CASE, 0x0139, 0x0139),
        (LOWER_CASE, 0x0139, 0x013A),
        (TITLE_CASE, 0x0139, 0x0139),
        // 013F;LATIN CAPITAL LETTER L WITH MIDDLE DOT;Lu;0;L;<compat> 004C 00B7;;;;N;;;;0140;
        (UPPER_CASE, 0x013f, 0x013f),
        (LOWER_CASE, 0x013f, 0x0140),
        (TITLE_CASE, 0x013f, 0x013f),
        // 0148;LATIN SMALL LETTER N WITH CARON;Ll;0;L;006E 030C;;;;N;LATIN SMALL LETTER N
        // HACEK;;0147;;0147
        (UPPER_CASE, 0x0148, 0x0147),
        (LOWER_CASE, 0x0148, 0x0148),
        (TITLE_CASE, 0x0148, 0x0147),
        // Lowercase lower than uppercase.
        // AB78;CHEROKEE SMALL LETTER GE;Ll;0;L;;;;;N;;;13A8;;13A8
        (UPPER_CASE, 0xab78, 0x13a8),
        (LOWER_CASE, 0xab78, 0xab78),
        (TITLE_CASE, 0xab78, 0x13a8),
        (UPPER_CASE, 0x13a8, 0x13a8),
        (LOWER_CASE, 0x13a8, 0xab78),
        (TITLE_CASE, 0x13a8, 0x13a8),
        // Last block in the 5.1.0 table
        // 10400;DESERET CAPITAL LETTER LONG I;Lu;0;L;;;;;N;;;;10428;
        (UPPER_CASE, 0x10400, 0x10400),
        (LOWER_CASE, 0x10400, 0x10428),
        (TITLE_CASE, 0x10400, 0x10400),
        // 10427;DESERET CAPITAL LETTER EW;Lu;0;L;;;;;N;;;;1044F;
        (UPPER_CASE, 0x10427, 0x10427),
        (LOWER_CASE, 0x10427, 0x1044F),
        (TITLE_CASE, 0x10427, 0x10427),
        // 10428;DESERET SMALL LETTER LONG I;Ll;0;L;;;;;N;;;10400;;10400
        (UPPER_CASE, 0x10428, 0x10400),
        (LOWER_CASE, 0x10428, 0x10428),
        (TITLE_CASE, 0x10428, 0x10400),
        // 1044F;DESERET SMALL LETTER EW;Ll;0;L;;;;;N;;;10427;;10427
        (UPPER_CASE, 0x1044F, 0x10427),
        (LOWER_CASE, 0x1044F, 0x1044F),
        (TITLE_CASE, 0x1044F, 0x10427),
        // First one not in the 5.1.0 table
        // 10450;SHAVIAN LETTER PEEP;Lo;0;L;;;;;N;;;;;
        (UPPER_CASE, 0x10450, 0x10450),
        (LOWER_CASE, 0x10450, 0x10450),
        (TITLE_CASE, 0x10450, 0x10450),
        // Non-letters with case.
        (LOWER_CASE, 0x2161, 0x2171),
        (UPPER_CASE, 0x0345, 0x0399),
    ];

    #[test]
    fn test_case() {
        for &(case, input, output) in CASE_TEST {
            if case == UPPER_CASE {
                assert_eq!(
                    unicode_to_upper(char::from_u32(input).unwrap()).unwrap() as u32,
                    output
                );
            } else if case == LOWER_CASE {
                assert_eq!(
                    unicode_to_lower(char::from_u32(input).unwrap()).unwrap() as u32,
                    output
                );
            } else {
                assert_eq!(
                    unicode_to_title(char::from_u32(input).unwrap()).unwrap() as u32,
                    output
                );
            }
        }
    }
}
