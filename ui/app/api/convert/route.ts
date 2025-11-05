import { NextRequest, NextResponse } from "next/server";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8100";

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const { query, from_sql, feature_flags } = body;

    const formData = new FormData();
    formData.append("query", query);
    formData.append("from_sql", from_sql);
    formData.append("to_sql", "e6");

    if (feature_flags) {
      formData.append("feature_flags", JSON.stringify(feature_flags));
    }

    const response = await fetch(`${API_URL}/convert-query`, {
      method: "POST",
      body: formData,
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
