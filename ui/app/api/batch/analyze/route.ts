import { NextRequest, NextResponse } from "next/server";
import type { BatchAnalyzeRequest } from "@/lib/types";

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8100";

export async function POST(request: NextRequest) {
  try {
    const body: BatchAnalyzeRequest = await request.json();

    const response = await fetch(`${API_BASE_URL}/api/v1/batch/analyze`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });

    const data = await response.json();

    if (!response.ok) {
      return NextResponse.json(
        { detail: data.detail || "Batch analysis failed" },
        { status: response.status }
      );
    }

    return NextResponse.json(data);
  } catch (error) {
    console.error("Batch analyze error:", error);
    return NextResponse.json(
      { detail: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 }
    );
  }
}
