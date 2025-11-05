import { NextRequest, NextResponse } from "next/server";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8100";

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const { query, from_sql, feature_flags } = body;

    const payload = {
      query,
      from_sql,
      to_sql: "e6",
      options: feature_flags || {}
    };

    const response = await fetch(`${API_URL}/api/v1/inline/analyze`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const error = await response.json();
      return NextResponse.json(error, { status: response.status });
    }

    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    return NextResponse.json(
      { detail: "Failed to convert query" },
      { status: 500 }
    );
  }
}
