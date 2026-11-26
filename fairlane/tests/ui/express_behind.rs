#[derive(fairlane::Lane)]
enum Urgency {
  Interactive,
  #[fairlane(express)]
  Wake,
}

fn main() {}
